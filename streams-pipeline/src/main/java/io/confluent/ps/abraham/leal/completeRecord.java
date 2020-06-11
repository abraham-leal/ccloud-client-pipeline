package io.confluent.ps.abraham.leal;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.ps.ServingRecord;
import io.confluent.ps.Tbf0Prescriber;
import io.confluent.ps.Tbf0Rx;
import io.confluent.ps.Tbf0RxTransaction;
import io.confluent.ps.abraham.leal.processors.ValueTransformerFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class completeRecord {

    public final static String rx_topic = "rx_topic_a";
    public final static String rx_t_topic = "rx_transactions_topic_a";
    public final static String ps_topic = "prescriber_topic_a";
    public final static String drug_topic = "drugs_topic";
    public final static String finalDestination = "complete_record_topic_ax";
    private static final Logger log = Logger.getRootLogger();

    public static Properties getConfig (){
        final Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "EnhanceRecord");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "<CCLOUD_BOOTSTRAP_SERVERS>");
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        streamsProps.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "3");
        streamsProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"<CCLOUD_SR_DNS>");
        streamsProps.put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG,"<SR_KEY>:<SR_SECRET>>");
        streamsProps.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO");
        streamsProps.put("security.protocol","SASL_SSL");
        streamsProps.put("sasl.mechanism","PLAIN");
        streamsProps.put("ssl.endpoint.identification.algorithm","https");
        streamsProps.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule " +
                "required username=\"<API_KEY>\" " +
                "password=\"<API_SECRET>\";");

        return streamsProps;
    }

    public static void main(String[] args) throws InterruptedException {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        final StreamsBuilder builder = new StreamsBuilder();
        //final ValueTransformerFactory transformMe = new ValueTransformerFactory();

        final Serde<ServingRecord> serveSerde = new SpecificAvroSerde<>();
        final Serde<Tbf0Rx> rxSerde = new SpecificAvroSerde<>();
        final Serde<Tbf0RxTransaction> rxtSerde = new SpecificAvroSerde<>();
        final Serde<Tbf0Prescriber> presSerde = new SpecificAvroSerde<>();
        final boolean isKeySerde = false;
        Map<String, Object> SRconfig = new HashMap<>();
        SRconfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"<CCLOUD_SR_DNS>");
        SRconfig.put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG,"<SR_KEY>:<SR_SECRET>");
        SRconfig.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO");
        serveSerde.configure(
                SRconfig,
                isKeySerde);
        rxSerde.configure(
                SRconfig,
                isKeySerde);
        rxtSerde.configure(
                SRconfig,
                isKeySerde);
        presSerde.configure(
                SRconfig,
                isKeySerde);

        //repartition prescribers
        builder.stream(ps_topic, Consumed.with(Serdes.String(),presSerde))
                .selectKey((k,v) -> Integer.toString(v.getPBRID())).to("repartitionedPrescriber");

        // Define topics to consume
        GlobalKTable<String,Tbf0Prescriber> ps_table = builder
                .globalTable("repartitionedPrescriber", Materialized.as("Prescriber_records"));
        KStream<String, Tbf0RxTransaction> rx_t_stream = builder
                .stream(rx_t_topic, Consumed.with(Serdes.String(),rxtSerde));
        KStream<String, Tbf0Rx> rx_stream = builder
                .stream(rx_topic, Consumed.with(Serdes.String(),rxSerde));

        // Repartition to needed keys
        KTable<String,Tbf0Rx> repartitionRXStream = rx_stream
                .selectKey((k,v) -> (String.join(Integer.toString(v.getRXNBR()),Integer.toString(v.getSTORENBR())))).toTable(Materialized.as("Rx_Begin_Store"));
        KTable<String,Tbf0RxTransaction> repartitionRXTStream = rx_t_stream
                .selectKey((k,v) -> (String.join(Integer.toString(v.getRXNBR()),Integer.toString(v.getSTORENBR())))).toTable(Materialized.as("RxT_Begin_Store"));

        // We will be bringing in the Prescriber Stream as a GlobalKTable instead
        //KTable<String,Tbf0Prescriber> repartitionPStream = ps_stream
        //       .selectKey((k,v) -> Integer.toString(v.getPBRID())).toTable(Materialized.as("Ps_Begin_Store"));

        // Pull in transaction eventing
        KTable<String,ServingRecord> enrichWithTransactions = repartitionRXStream
                .leftJoin(repartitionRXTStream,getTransactions());

        KStream<String,ServingRecord> enrichedWithTransactionsStream = enrichWithTransactions.toStream();

        // Prescribers as a GlobalKTable Joined to our record to produce a complete record

        KStream<String, ServingRecord> finalRecord = enrichedWithTransactionsStream.leftJoin(ps_table,
                (k,v) -> v.getPrescriberId().toString(),
                getPrescriberInfo())
                .selectKey((k,v) -> (createJSONKey(v.getRxNumber(),v.getStoreNumber())));


        // Old Prescriber handling
        /*
        KTable<String,ServingRecord> repartitionEnrichedValues = enrichWithTransactions.toStream()
                .selectKey((k,v) -> v.getPrescriberId().toString()).toTable();

        KTable<String, ServingRecord> enrichWithPrescriber = repartitionEnrichedValues
                .leftJoin(repartitionPStream,getPrescriberInfo());

        KTable<String, ServingRecord> readyRecord = enrichWithPrescriber.toStream()
                .selectKey((k,v) -> (createJSONKey(v.getRxNumber(),v.getStoreNumber())))
                .toTable();
         */

        // Send data back to Kafka
        finalRecord.to(finalDestination, Produced.with(Serdes.String(),serveSerde));

        //Build topology
        final KafkaStreams streams = new KafkaStreams(builder.build(), getConfig());

        try {
            streams.start();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static String createJSONKey(CharSequence key1, int key2){
        JSONObject toReturn = new JSONObject();
        toReturn.put("RxNum",key1);
        toReturn.put("StoreNbr",Integer.toString(key2));
        return toReturn.toString();
    }

    private static ValueJoiner<Tbf0Rx, Tbf0RxTransaction, ServingRecord> getTransactions(){
        return new ValueJoiner<Tbf0Rx, Tbf0RxTransaction, ServingRecord>() {
            @Override
            public ServingRecord apply(Tbf0Rx leftValue, Tbf0RxTransaction rightValue) {
                ServingRecord creationPoint = new ServingRecord();
                try {
                    creationPoint.setPatId((Integer.toString(leftValue.getPATID())));
                    creationPoint.setRxNumber(Integer.toString(leftValue.getRXNBR()));
                    creationPoint.setStoreNumber(leftValue.getSTORENBR());
                    creationPoint.setDaysSupply(leftValue.getFILLDAYSSUPPLY());
                    creationPoint.setPrescribedFillCount(leftValue.getFILLNBRPRESCRIBED());
                    creationPoint.setDispensedFillCount(leftValue.getFILLNBRDISPENSED());
                    creationPoint.setLastFillPartialFlag(leftValue.getPARTIALFILLCD()); // Is this right? It returns a number
                    creationPoint.setRemainingFillCount(0);
                    creationPoint.setQuantity(leftValue.getRXORIGINALQTY());
                    if(rightValue != null){
                        creationPoint.setPartialFilledQuanity(rightValue.getPARTIALFILINTNDEDQTY());
                        creationPoint.setFillQuantityDispensed(Integer.toString(rightValue.getFILLQTYDISPENSED()));
                    }else {
                        creationPoint.setPartialFilledQuanity(0);
                        creationPoint.setFillQuantityDispensed("InfoNotYetAvailable");
                    }
                    creationPoint.setLastFillDate(leftValue.getFILLENTEREDDTTM());
                    creationPoint.setNextFillDate(leftValue.getRXREFILLSBYDTTM()); //
                    creationPoint.setRxSig(leftValue.getRXSIG()); // Further Processing needed?
                    creationPoint.setPrescriberPhone("TobeSet");
                    creationPoint.setPrescriberName(leftValue.getPBRLASTNAME());
                    creationPoint.setPrescriberId(Integer.toString(leftValue.getPBRID()));
                    creationPoint.setNonSystemCompoundInd(leftValue.getDRUGNONSYSTEMCD());
                    creationPoint.setRefillable(false);
                    creationPoint.setHideRx("false");
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
                    System.exit(1);
                }
                return creationPoint;
            }
        };
    }

    private static ValueJoiner<ServingRecord, Tbf0Prescriber, ServingRecord> getPrescriberInfo(){
        return new ValueJoiner<ServingRecord, Tbf0Prescriber, ServingRecord>() {
            @Override
            public ServingRecord apply(ServingRecord leftValue, Tbf0Prescriber rightValue) {
                try {
                    if (rightValue != null){
                        String PBR_Phone = (rightValue.getPBRPHONEAREACD().toString() + rightValue.getPBRPHONE().toString());
                        leftValue.setPrescriberPhone(PBR_Phone);
                    }else {
                        leftValue.setPrescriberPhone("Null");
                    }
                }
                catch (Exception e){
                    System.out.println("Could Not Parse and add to finalValue from left stream in prescribers");
                    e.printStackTrace();
                    System.exit(1);
                }
                return leftValue;
            }
        };
    }
}