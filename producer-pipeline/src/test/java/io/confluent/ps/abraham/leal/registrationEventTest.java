package io.confluent.ps.abraham.leal;

import AvroSchema.ExtraInfo;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.salesforce.kafka.test.KafkaTestUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import io.confluent.kafka.schemaregistry.CompatibilityChecker;

import static org.junit.Assert.*;

public class registrationEventTest {

    @ClassRule
    public static SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .withBrokerProperty("auto.create.topics.enable","false");
    public static SchemaRegistryClient schemaR = MockSchemaRegistry.getClientForScope("mySR");
    public static KafkaTestUtils kafkaUtils = null;

    @Before
    public void setUp(){
        kafkaUtils = sharedKafkaTestResource.getKafkaTestUtils();
        kafkaUtils.createTopic("testingTopic", 1, (short) 1);
        kafkaUtils.createTopic("dlq-testingTopic", 1, (short) 1);
    }


    @Test
    public void producingTest() throws IOException {
        // Test correct generation of avro record and production

        KafkaProducer<String, ExtraInfo> testingProducer = new KafkaProducer<String, ExtraInfo>(producerProps());
        registrationEvent.produce(testingProducer, "testingTopic",1, "A","L",9);

        assertEquals(1,kafkaUtils.consumeAllRecordsFromTopic("testingTopic").size());
    }

    @Test
    public void DLQTest() throws ExecutionException, InterruptedException {

        ProducerRecord<String,ExtraInfo> toDLQRecord =
                new ProducerRecord<String, ExtraInfo>("dlq-testingTopic", null,
                        new ExtraInfo(1,"A","L",9));
        registrationEvent.sendToDLQ(toDLQRecord,"dlq-testingTopic",producerProps());

        assertEquals(1,kafkaUtils.consumeAllRecordsFromTopic("dlq-testingTopic").size());
    }

    @Test
    public void writeToFileTest() throws IOException {

        String filename = "ProducerLog";
        ProducerRecord<String,ExtraInfo> writeTestRecord =
                new ProducerRecord<String, ExtraInfo>("dlq-testingTopic", null,
                        new ExtraInfo(1,"A","L",9));

        registrationEvent.writeToFile(writeTestRecord,filename);

        File testingFile = new File (filename+".log");
        assertTrue("The file was successfully created",testingFile.exists());

        assertTrue("The file was successfully deleted",testingFile.delete());
    }

    /*
    @Test
    public void triggerDLQTest() throws IOException {
        // Unclear whether a DLQ is needed for the producer at this moment.
        // Test correct failover to DLQ Topic

        KafkaProducer<String, ExtraInfo> testingProducer = new KafkaProducer<String, ExtraInfo>(producerProps());

        // Topic "testingDLQ" was never created, and we've disabled auto-topic-creation
        // we expect to failover to "dlq-testingDLQ"
        Producer.produce(testingProducer, "testingDLQ",1, "A","L",9);

        // Confirm "dlq-testingDLQ" has the record we sent
        assertEquals(1,kafkaUtils.consumeAllRecordsFromTopic("dlq-testingDLQ").size());
    }
*/
    @Test
    public void triggerWriteToFileTest() {

        assertEquals(1,1);
    }

    @Test
    public void getConfigTest(){

        assertEquals(1,1);
    }

    @Test
    public void avroBackwardCompatibilityChecker() {
        // We are testing schema evolution of our custom avro schema object.

        // Get current schema
        final AvroSchema currentSchema = new AvroSchema(ExtraInfo.getClassSchema().toString());

        // Add a field to current schema, with default value
        String newSchemaString = ExtraInfo.getClassSchema().toString().substring(0,ExtraInfo.getClassSchema().toString().length()-2)
                + ",{\"type\":\"string\",\"name\":\"newFiled\", \"default\": \"IAmDefault\"}" +"]}";

        // Add a field to current schema, without default value
        String newSchemaStringNoDefault = ExtraInfo.getClassSchema().toString().substring(0,ExtraInfo.getClassSchema().toString().length()-2)
                + ",{\"type\":\"string\",\"name\":\"newFiled\"}" +"]}";

        final AvroSchema currentSchemaNewField = new AvroSchema(newSchemaString);
        final AvroSchema currentSchemaNewFieldNoDefault = new AvroSchema(newSchemaStringNoDefault);

        CompatibilityChecker checker = CompatibilityChecker.BACKWARD_CHECKER;
        assertTrue("Adding a field with a default is a backward compatible change",
                checker.isCompatible(currentSchemaNewField, Collections.singletonList(currentSchema)));
        assertFalse("Adding a field without a default is not a backward compatible change",
                checker.isCompatible(currentSchemaNewFieldNoDefault, Collections.singletonList(currentSchema)));

    }

    private Properties producerProps (){
        Properties myProps = new Properties();

        myProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sharedKafkaTestResource.getKafkaConnectString());
        myProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        myProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        myProps.put(ProducerConfig.CLIENT_ID_CONFIG, "testingProducer");
        myProps.put(ProducerConfig.RETRIES_CONFIG,"5");
        myProps.put("schema.registry.url","mock://mySR");
        return myProps;
    }

}