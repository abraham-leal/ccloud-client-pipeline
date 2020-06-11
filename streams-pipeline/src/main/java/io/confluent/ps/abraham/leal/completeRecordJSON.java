package io.confluent.ps.abraham.leal;

import io.confluent.gen.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.json.JSONObject;

public class completeRecordJSON {

    public final static String rx_topic = "rx";
    public final static String rx_t_topic = "rx_trans";
    public final static String ps_topic = "prescriber";
    public final static String finalDestination = "complete_record";

    public static StreamsBuilder getTopology (Serde<ServingRecord> serveSerde, Serde<Tbf0Prescriber> presSerde,
                                              Serde<Tbf0Rx> rxSerde, Serde<Tbf0RxTransaction> rxtSerde){

        Logger logger = Logger.getRootLogger();
        BasicConfigurator.configure();

        final StreamsBuilder builder = new StreamsBuilder();
        //final ValueTransformerFactory transformMe = new ValueTransformerFactory();

        //repartition prescribers
        KStream<String, Tbf0Prescriber> repartitionPres = builder.stream(ps_topic, Consumed.with(Serdes.String(), presSerde));
        repartitionPres.selectKey((k,v) -> Integer.toString(v.getPBRID())).to("repartitionedPrescriber");

        // Define topics to consume
        GlobalKTable<String,Tbf0Prescriber> ps_table = builder
                .globalTable("repartitionedPrescriberA", Materialized.as("Prescriber_records"));
        KStream<String, Tbf0RxTransaction> rx_t_stream = builder
                .stream(rx_t_topic, Consumed.with(Serdes.String(), rxtSerde));

        KStream<String, Tbf0Rx> rx_stream = builder
                .stream(rx_topic, Consumed.with(Serdes.String(), rxSerde));

        // Repartition to needed keys
        KTable<String,Tbf0Rx> repartitionRXStream = rx_stream
                .selectKey((k,v) -> (String.join(Integer.toString(v.getRXNBR()),Integer.toString(v.getSTORENBR())))).toTable(Materialized.with(Serdes.String(),rxSerde));
        KTable<String,Tbf0RxTransaction> repartitionRXTStream = rx_t_stream
                .selectKey((k,v) -> (String.join(Integer.toString(v.getRXNBR()),Integer.toString(v.getSTORENBR())))).toTable(Materialized.with(Serdes.String(),rxtSerde));

        // Pull in transaction eventing
        KTable<String,ServingRecord> enrichWithTransactions = repartitionRXStream
                .leftJoin(repartitionRXTStream,getTransactions());

        KStream<String,ServingRecord> enrichedWithTransactionsStream = enrichWithTransactions.toStream();

        // Prescribers as a GlobalKTable Joined to our record to produce a complete record

        KStream<String, ServingRecord> finalRecord = enrichedWithTransactionsStream.leftJoin(ps_table,
                (k,v) -> v.getPrescriberId(),
                getPrescriberInfo())
                .selectKey((k,v) -> (createJSONKey(v.getRxNumber(),v.getStoreNumber())));

        // Send data back to Kafka
        logger.info("Successfully produced a record");
        finalRecord.to(finalDestination, Produced.with(Serdes.String(),serveSerde));

        return builder;

    }

    private static String createJSONKey(CharSequence key1, int key2){
        JSONObject toReturn = new JSONObject();
        toReturn.put("RxNum",key1);
        toReturn.put("StoreNbr",Integer.toString(key2));
        return toReturn.toString();
    }

    private static ValueJoiner<Tbf0Rx, Tbf0RxTransaction, ServingRecord> getTransactions(){
        return (leftValue, rightValue) -> {
            ServingRecord creationPoint = new ServingRecord();
            System.out.println(leftValue);
            System.out.println(rightValue);
            try {
                creationPoint.setPatId((Integer.toString(leftValue.getPATID())));
                creationPoint.setRxNumber(Integer.toString(leftValue.getRXNBR()));
                creationPoint.setStoreNumber(leftValue.getSTORENBR());
                creationPoint.setDaysSupply(leftValue.getFILLDAYSSUPPLY());
                creationPoint.setPrescribedFillCount(leftValue.getFILLNBRPRESCRIBED());
                creationPoint.setDispensedFillCount(leftValue.getFILLNBRDISPENSED());
                creationPoint.setLastFillPartialFlag(leftValue.getPARTIALFILLCD() == 0); // Is this right? It returns a number
                creationPoint.setRemainingFillCount(0);
                creationPoint.setQuantity(leftValue.getRXORIGINALQTY());
                if(rightValue != null){
                    creationPoint.setPartialFilledQuanity(rightValue.getPARTIALFILINTNDEDQTY());
                    creationPoint.setFillQuantityDispensed(rightValue.getFILLQTYDISPENSED());
                }else{
                    creationPoint.setPartialFilledQuanity(0);
                    creationPoint.setFillQuantityDispensed(0);
                }
                creationPoint.setPartialFilledQuanity(0);
                creationPoint.setFillQuantityDispensed(0);
                creationPoint.setLastFillDate(leftValue.getFILLENTEREDDTTM());
                creationPoint.setNextFillDate(leftValue.getRXREFILLSBYDTTM()); //
                creationPoint.setRxSig(leftValue.getRXSIG()); // Further Processing needed?
                creationPoint.setPrescriberPhone(0);
                creationPoint.setPrescriberName(leftValue.getPBRLASTNAME());
                creationPoint.setPrescriberId(Integer.toString(leftValue.getPBRID()));
                creationPoint.setNonSystemCompoundInd(leftValue.getDRUGNONSYSTEMCD());
                creationPoint.setRefillable(false);
                creationPoint.setHideRx(false);
                creationPoint.setNinetyFillFlag("nononono");
                creationPoint.setNinetyDayPrefInd(leftValue.getRX90DAYPREFIND());
                creationPoint.setAutoRefillInd("dontdoit");
                creationPoint.setAutoRefillPrefInd(leftValue.getFILLAUTOIND());
                creationPoint.setRemainingFillCount(0);
                creationPoint.setRefillable(false);
                creationPoint.setNinetyFillFlag(String.valueOf(false));
                creationPoint.setAutoRefillInd(String.valueOf(false));
            }
            catch (Exception e){
                System.out.println("Could Not Parse and add to finalValue from left stream");
                e.printStackTrace();
                System.exit(0);
            }
            return creationPoint;
        };
    }

    private static ValueJoiner<ServingRecord, Tbf0Prescriber, ServingRecord> getPrescriberInfo(){
        return (leftValue, rightValue) -> {
            try {
                if (rightValue != null){
                    String PBR_Phone = (rightValue.getPBRPHONEAREACD() + rightValue.getPBRPHONE());
                    leftValue.setPrescriberPhone(Integer.getInteger(PBR_Phone));
                }else {
                    leftValue.setPrescriberPhone(0);
                }
            }
            catch (Exception e){
                System.out.println("Could Not Parse and add to finalValue from left stream in prescribers");
                e.printStackTrace();
                System.exit(1);
            }
            return leftValue;
        };
    }

}