package io.confluent.ps.abraham.leal;

import AvroSchema.ExtraInfo;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.salesforce.kafka.test.KafkaBroker;
import com.salesforce.kafka.test.KafkaTestUtils;

import java.util.Collections;

import io.confluent.kafka.schemaregistry.CompatibilityChecker;

import static org.junit.Assert.*;

public class ProducerCCloudTest {

    @ClassRule
    public static SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();
    public static SchemaRegistryClient schemaR = MockSchemaRegistry.getClientForScope("mySR");
    public static KafkaTestUtils kafkaUtils = null;

    @Before
    public void setUp(){
        kafkaUtils = sharedKafkaTestResource.getKafkaTestUtils();
        kafkaUtils.createTopic("testingTopic", 1, (short) 1);
    }


    @Test
    public void mainTest() {
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

    @Test
    public void getConfigTest(){
        assertEquals(1,1);
    }

}