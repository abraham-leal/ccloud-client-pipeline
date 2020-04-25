package io.confluent.ps.abraham.leal;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import AvroSchema.ExtraInfo;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.errors.RetriableException;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.store;
/*
This producer takes two arguments: a --producer-props flag declaring a file with all necessary connection info (auth info for CCloud cluster
as well as the cluster's connection string) and a --topic flag declaring which topic to produce to

Sample Config File:

# Kafka
bootstrap.servers=
security.protocol=SASL_SSL
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule   required username="<APIKEY>"   password="<APISECRET>";
ssl.endpoint.identification.algorithm=https
sasl.mechanism=PLAIN
# Confluent Cloud Schema Registry
schema.registry.url=
basic.auth.credentials.source=USER_INFO
schema.registry.basic.auth.user.info=<APIKEYSR>:<APISECRETSR>
*/

public class Producer {

    public static  Random getNum = new Random();
    public static Logger log = LoggerFactory.getLogger(Producer.class);
    private static Properties securityConfiguration;
    private static String topic;

    // Command Line Argument Parser
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newFor("ccloud-producer").build();

        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("produce messages to this topic");

        parser.addArgument("--producer-props")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("CONFIG-FILE")
                .dest("producerConfigFile")
                .help("producer config properties file.");

        return parser;
    }

    //Parse file and load properties
    private static Properties loadProps(String filename) throws IOException {
        Properties props = new Properties();
        try (InputStream propStream = Files.newInputStream(Paths.get(filename))) {
            props.load(propStream);
        }
        return props;
    }

    public static Properties getConfig (Properties securityConfigs) throws IOException {
        final Properties props = new Properties();
        // NOTE: All Connection related properties are passed at runtime for security through JVM parameters
        props.putAll(securityConfigs);

        //Producer Performance Configs
        props.put(ProducerConfig.ACKS_CONFIG, "-1");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "ProducerRun");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 500);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        return props;
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException, ArgumentParserException {

        // Set INFO level logging
        BasicConfigurator.configure();

        ArgumentParser parser = argParser();
        KafkaProducer<String, ExtraInfo> producer = null;

        try {
            Namespace res = parser.parseArgs(args);

            // Parsing values passed to the application
            String producerProps = res.getString("producerConfigFile");
            topic = res.getString("topic");

            securityConfiguration = loadProps(producerProps);
            producer = new KafkaProducer<String, ExtraInfo>(getConfig(securityConfiguration));

            if (producerProps == null) {
                throw new ArgumentParserException("--producer-props must be specified.", parser);
            }

            if (topic == null) {
                throw new ArgumentParserException("--topic must be specified.", parser);
            }

            //Production Loop
            while(true) { // Passing generated data by helper methods to the producing method
                produce(producer, topic, getID(), getName(), getFood(), getPC());
            }

        }
        // Error Handling
        catch (ArgumentParserException a){ // Simple message for alerting of bad arguments given
            log.info("Producer could not initialize.");
            log.info("Error parsing given attributes, make sure you are passing a valid file to " +
                    "--producer-props and a non-special topic name to --topic \"[a-zA-Z0-9\\\\._\\\\-]\"");
            a.printStackTrace();
        }
        catch(OutOfMemoryError m){ // Out of memory errors are common when the producer is aggregating too many records
            // This assures an informative message to the operator to increase the heap.
            // Or possibly trigger a question: Am I sending my data frequently enough?
            if (producer != null){
                log.info("Producer has exited with an out of memory error, most likely because " +
                        "the internal aggregator has filled up faster than it can send messages to kafka.");
                log.info("Flushing Aggregator to Broker...");
                producer.flush();
                log.info("Flushed pending messages, try restarting the producer with a larger heap to " +
                        "handle the production of messages.");
            }
            else{
                log.info("Producer could not initialize. Trying giving the producer a larger heap.");
            }
        }
        if (producer!=null){
            log.info("Successfully produced messages");
            //Shutdown hook to assure producer close
            Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
        }
    }

    // Production method
    // It is important to decouple production from the main method in order to be able to unit test properly
    // Much of the error handling is happening here
    public static void produce (KafkaProducer<String,ExtraInfo> producer, String topic, int Id, String name, String food, int pc){
        final ExtraInfo recordValue = new ExtraInfo(Id, name, food, pc);
        final ProducerRecord<String, ExtraInfo> record = new ProducerRecord<String, ExtraInfo>(topic, null, recordValue);
        producer.send(record, (recordMetadata, exception) -> {
            if (exception != null){
                if (exception instanceof RetriableException){ // We've received a retriable exception to trying to send
                    // a record. Due to this, we will try to handle the record in two ways:
                    // 1. Send to a DLQ topic where it may be available and be retried again
                    // 2. Flush to disk for later pick up by a scraper
                    // A third option may be to send the record upstream, but since we generate our records locally,
                    // this isn't really an option
                    log.warn("Production has failed after the set amount of retries. Sending to DLQ...");
                    try {
                        if (!sendToDLQ(record, "dlq-" + topic, securityConfiguration)) { // Send to DLQ topic, else write to local file
                            log.warn("DLQ is unreachable, flushing to disk at in local directory");
                            writeToFile(record);
                        }
                    } catch (ExecutionException | InterruptedException e) { // Handle not being able to produce to DLQ synchronously
                        log.warn("Producer was interrupting while sending to DLQ. Aborting.");
                        e.printStackTrace();
                    }catch (IOException i){ //Handle not being able to write to file in local directory
                        log.warn("Could not write to file in local path to handle undelivered record. Aborting.");
                        i.printStackTrace();
                    }

                } else { //Handle Unretriable Exceptions
                    log.warn("Production to topic " + recordMetadata.topic() + "Has failed unrecoverably. Ending Producer.");
                    exception.printStackTrace();
                }
        }
        });
    }

    public static boolean sendToDLQ (ProducerRecord<String,ExtraInfo> failedRecord, String dlqTopic, Properties securityConfiguration) throws ExecutionException, InterruptedException {
        // Here, we generate properties for a DLQ producer, which will send the record synchronously to assure delivery.
        // If the producer cannot confirm delivery by the brokers, it will report back failure
        // NOTE: This is a blocking send in the I/O Thread, but provides guarantees the record will be delivered for later retrieval to the brokers

        Properties dlqprops = new Properties();
        dlqprops.putAll(securityConfiguration);
        dlqprops.put(ProducerConfig.ACKS_CONFIG, "-1");
        dlqprops.put(ProducerConfig.RETRIES_CONFIG, 5);
        dlqprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        dlqprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        dlqprops.put(ProducerConfig.CLIENT_ID_CONFIG, "DLQProducer");
        dlqprops.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
        dlqprops.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        dlqprops.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        KafkaProducer<String,ExtraInfo> toDLQ = new KafkaProducer<>(dlqprops);
        final ProducerRecord<String, ExtraInfo> recordToSend =
                new ProducerRecord<String,ExtraInfo>(dlqTopic, failedRecord.key(),failedRecord.value());
        RecordMetadata sentDLQ = toDLQ.send(recordToSend).get();
        toDLQ.close();
        if (sentDLQ != null){
            return true;
        }
        else {
            return false;
        }
    }

    public static void writeToFile (ProducerRecord<String,ExtraInfo> recordToWrite) throws IOException {
        // This method appends unsent records to a file in the local directory
        File unsentLog = new File("ProducerLog.log");
        if (unsentLog.exists()) {
            Files.write(Paths.get("ProducerLog.log"), recordToWrite.toString().getBytes(), StandardOpenOption.APPEND);
        }else{
            unsentLog.createNewFile();
            Files.write(Paths.get("ProducerLog.log"), recordToWrite.toString().getBytes(), StandardOpenOption.APPEND);
        }
    }

    // Helper methods to generate fake data.

    public static Integer getID (){
        return getNum.nextInt(10000 - 1000) + 1000;
    }

    public static String getName(){
        String [] x = {"Abraham", "Russell", "Bob", "Mark","Jay","Valeria", "Miguel", "Jaime"};
        return x[getNum.nextInt(x.length)];
    }

    public static String getFood(){
        String [] x = {"Hot Dog", "Pizza", "Fries", "Burger","Chicken Strips","Salad"};
        return x[getNum.nextInt(x.length)];
    }

    public static Integer getPC (){
        return getNum.nextInt(79999 - 7000) + 70000;
    }


}
