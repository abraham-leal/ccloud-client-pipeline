package io.confluent.ps.abraham.leal;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import AvroSchema.ExtraInfo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Random;

import static java.util.logging.Level.INFO;
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

public class ProducerCCloud {

    public static  Random getNum = new Random();
    public static Logger log = LoggerFactory.getLogger(ProducerCCloud.class);

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

            // Parse values
            String producerProps = res.getString("producerConfigFile");
            String topic = res.getString("topic");

            Properties securityConfigs = loadProps(producerProps);
            producer = new KafkaProducer<String, ExtraInfo>(getConfig(securityConfigs));

            if (producerProps == null) {
                throw new ArgumentParserException("--producer-props must be specified.", parser);
            }

            if (topic == null) {
                throw new ArgumentParserException("--topic must be specified.", parser);
            }

            //Production Loop
            produce(producer, topic, getID(), getName(), getFood(), getPC());
            log.info("Successfully produced messages");

        }
        // Error Handling
        catch (org.apache.kafka.common.errors.RetriableException e){
            log.info("The producer retried sending a batch of messages, but was unable to," +
                    " upping retires, then restarting");
            Properties props = loadProps(parser.parseArgs(args).getString("producerConfigFile"));
            props.put(ProducerConfig.BATCH_SIZE_CONFIG,String.valueOf(Integer.getInteger(getConfig(props).getProperty("retries"))+5));
            producer = new KafkaProducer<String, ExtraInfo>(getConfig(props));
            produce(producer,parser.parseArgs(args).getString("topic"),getID(),getName(),getFood(),getPC());

        }
        catch (ArgumentParserException a){
            log.info("Producer could not initialize.");
            log.info("Error parsing given attributes, make sure you are passing a valid file to " +
                    "--producer-props and a non-special topic name to --topic \"[a-zA-Z0-9\\\\._\\\\-]\"");
            a.printStackTrace();
        }
        catch(OutOfMemoryError m){
            if (producer != null){
                log.info("Producer has exited with an out of memory error, most likely because " +
                        "the internal aggregator has filled up faster than it can send messages to kafka.");
                log.info("Flushing Aggregator to Broker...");
                producer.flush();
                log.info("Flushed pending messages, try restarting the producer with a larger heap to handle all the production of messages.");
                //Shutdown hook to assure producer close
                Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
            }
            else{
                log.info("Producer could not initialize. Trying giving the producer a larger heap.");
            }
        }

    }

    public static void produce (KafkaProducer<String,ExtraInfo> producer, String topic, int Id, String name, String food, int pc){
        while (true) {
            final ExtraInfo recordValue = new ExtraInfo(Id, name, food, pc);
            final ProducerRecord<String, ExtraInfo> record = new ProducerRecord<String, ExtraInfo>(topic, null, recordValue);
            producer.send(record);
        }
    }

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
