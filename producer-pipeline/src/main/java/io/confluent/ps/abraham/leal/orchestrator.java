package io.confluent.ps.abraham.leal;

import kafka.Kafka;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class orchestrator {

    private static Properties securityConfiguration;
    public static Logger log = LoggerFactory.getLogger(purchaseEvent.class);

    public static void main(String[] args) throws IOException {


        // Set INFO level logging
        BasicConfigurator.configure();
        ArgumentParser parser = argParser();
        try {
            Namespace res = parser.parseArgs(args);
            // Parsing values passed to the application
            String producerProps = res.getString("producerConfigFile");
            if (producerProps == null) {
                throw new ArgumentParserException("--producer-props must be specified.", parser);
            }
            securityConfiguration = loadProps(producerProps);
        }
        // Error Handling
        catch (ArgumentParserException | IOException a){ // Simple message for alerting of bad arguments given
            log.info("Producer could not initialize.");
            log.info("Error parsing given attributes, make sure you are passing a valid file to " +
                    "--producer-props and a non-special topic name to --topic \"[a-zA-Z0-9\\\\._\\\\-]\"");
            a.printStackTrace();
        }

        KafkaProducer<String, SpecificRecordBase> reusableProducer =
                new KafkaProducer<String, SpecificRecordBase>(getConfig(securityConfiguration));

        Thread[] runners = new Thread[1];
        for (int i = 0; i < 2; i++) {
            runners[i] = new Thread(new purchaseEvent(reusableProducer,securityConfiguration));
            runners[i].start();
        }

        try {
            for (Thread t : runners)
                t.join();
        } catch (InterruptedException e) {
            log.error("Thread Interrupted ");
        } finally {
            reusableProducer.close();
            log.info("Finished dispatcher demo - Closing Kafka Producer.");
        }

    }

    // Command Line Argument Parser
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newFor("ccloud-producer").build();

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
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "purchasesAndRegistrationsProducer");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 10000);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,"300000");

        return props;
    }


}
