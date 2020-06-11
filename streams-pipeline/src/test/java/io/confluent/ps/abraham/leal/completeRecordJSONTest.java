package io.confluent.ps.abraham.leal;


import io.confluent.gen.ServingRecord;
import io.confluent.gen.Tbf0Prescriber;
import io.confluent.gen.Tbf0Rx;
import io.confluent.gen.Tbf0RxTransaction;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

public class completeRecordJSONTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Tbf0Prescriber> prescriber;
    private TestInputTopic<String, Tbf0Rx> rx;
    private TestInputTopic<String, Tbf0RxTransaction> rx_trans;
    private TestOutputTopic<String, ServingRecord> complete_record;
    private Tbf0Rx myRxValue = new Tbf0Rx();
    private Tbf0RxTransaction myRxtValue = new Tbf0RxTransaction();
    private Tbf0Prescriber myPresValue = new Tbf0Prescriber();
    private TestOutputTopic<String, Tbf0Prescriber> repartitionedPrescriber;
    private MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();

    @Before
    public void SetUp() {
        BasicConfigurator.configure();


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "CompleteRecord");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://mockSR");
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,true);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);

        // Set up custom serdes to configure topics with
        final Serde<ServingRecord> serveSerde = getSerdeConfigs("serve");
        final Serde<Tbf0Prescriber> presSerde = getSerdeConfigs("pres");
        final Serde<Tbf0Rx> rxSerde = getSerdeConfigs("rx");
        final Serde<Tbf0RxTransaction> rxtSerde = getSerdeConfigs("trans");

        Topology myStream = completeRecordJSON.getTopology(serveSerde,presSerde,rxSerde,rxtSerde).build();
        testDriver = new TopologyTestDriver(myStream, props);

        // Create Topics to test with
        rx = testDriver.createInputTopic("rx", Serdes.String().serializer(), rxSerde.serializer());
        rx_trans = testDriver.createInputTopic("rx_trans", Serdes.String().serializer(), rxtSerde.serializer());
        prescriber = testDriver.createInputTopic("prescriber", Serdes.String().serializer(), presSerde.serializer());
        complete_record = testDriver.createOutputTopic("complete_record", Serdes.String().deserializer(), serveSerde.deserializer());
        repartitionedPrescriber = testDriver.createOutputTopic("repartitionedPrescriber", Serdes.String().deserializer(), presSerde.deserializer());

        // Fill dummy records
        myRxValue.setRXNBR(900);
        myRxValue.setSTORENBR(20);
        myRxValue.setPBRID(100);
        myRxValue.setPBRLOCID(2000);
        myRxValue.setDRUGCLASS("ANewEmpire");

        myRxtValue.setPARTIALFILINTNDEDQTY(5);
        myRxtValue.setFILLQTYDISPENSED(20);
        myRxtValue.setRXNBR(900);
        myRxtValue.setSTORENBR(20);

        myPresValue.setPBRPHONEAREACD("512");
        myPresValue.setPBRPHONE("5555555");
        myPresValue.setPBRID(100);

    }

    @Test
    public void shouldReturnNoRecord(){
        prescriber.pipeInput(myPresValue);
        assertTrue(complete_record.isEmpty());
        assertFalse(repartitionedPrescriber.isEmpty());
        rx_trans.pipeInput(myRxtValue);
        assertTrue(complete_record.isEmpty());
    }

    @Test
    public void shouldReturnComplete() {
        prescriber.pipeInput(myPresValue);
        rx_trans.pipeInput(myRxtValue);
        rx.pipeInput(myRxValue);
        assertFalse(complete_record.isEmpty());
    }

    @After
    public void tearDown(){
        testDriver.close();
    }

    public static Serde getSerdeConfigs (String className){
        final Serde<ServingRecord> serveSerde = new KafkaJsonSchemaSerde<>();
        final Serde<Tbf0Prescriber> presSerde = new KafkaJsonSchemaSerde<>();
        final Serde<Tbf0Rx> rxSerde = new KafkaJsonSchemaSerde<>();
        final Serde<Tbf0RxTransaction> rxtSerde = new KafkaJsonSchemaSerde<>();

        Map<String, Object> SRConfig = new HashMap<>();
        SRConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://mockSR");

        Map<String,Object> serveSerdeConfig = new HashMap<>();
        serveSerdeConfig.putAll(SRConfig);
        serveSerdeConfig.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, ServingRecord.class);
        serveSerde.configure(SRConfig, false);

        Map<String, Object> SRconfigPres = new HashMap<>();
        SRconfigPres.putAll(SRConfig);
        SRconfigPres.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Tbf0Prescriber.class);
        presSerde.configure(SRconfigPres, false);

        Map<String, Object> SRconfigRx = new HashMap<>();
        SRconfigRx.putAll(SRConfig);
        SRconfigRx.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Tbf0Rx.class);
        rxSerde.configure(SRconfigRx, false);

        Map<String, Object> SRconfigRxT = new HashMap<>();
        SRconfigRxT.putAll(SRConfig);
        SRconfigRxT.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Tbf0RxTransaction.class);
        rxtSerde.configure(SRconfigRxT, false);

        switch (className){
            case "serve":
                return serveSerde;
            case "pres":
                return presSerde;
            case "trans":
                return rxtSerde;
            case "rx":
                return rxSerde;
            default:
                throw new IllegalArgumentException("Not a registered serde.");
        }
    }
}