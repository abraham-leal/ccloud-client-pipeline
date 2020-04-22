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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Random;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class ProducerCCloud {

    public static  Random getNum = new Random();

    public static Properties getConfig (String topic, Properties security) throws IOException {
        final Properties props = new Properties();
        // NOTE: All Connection related properties are passed at runtime for security through JVM parameters
        props.putAll(security);

        //Remaining Configs

        props.put(ProducerConfig.ACKS_CONFIG, "-1");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "MyProducer");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 50);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        return props;
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        ArgumentParser parser = argParser();
        KafkaProducer<String, ExtraInfo> producer = null;

        try {
            Namespace res = parser.parseArgs(args);

            /* parse */
            String producerProps = res.getString("producerConfigFile");
            String topic = res.getString("topic");

            Properties security = loadProps(producerProps);

            producer = new KafkaProducer<String, ExtraInfo>(getConfig(topic, security));

            if (producerProps == null) {
                throw new ArgumentParserException("--producer-props must be specified.", parser);
            }

            if (topic == null) {
                throw new ArgumentParserException("--topic must be specified.", parser);
            }


            while (true) {
                final ExtraInfo recordValue = new ExtraInfo(getID(), getName(), getFood(), getPC());
                final ProducerRecord<String, ExtraInfo> record = new ProducerRecord<String, ExtraInfo>(topic, null, recordValue);
                producer.send(record);
            }

        }
        catch (ArgumentParserException a){
            //Tell producer to flush before exiting
            if (producer != null){
                producer.flush();
                //Shutdown hook to assure producer close
                Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
            }

            a.printStackTrace();
        }

        System.out.printf("Successfully produced messages");



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

    /** Get the command-line argument parser. */
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

    public static Properties loadProps(String filename) throws IOException {
        Properties props = new Properties();
        try (InputStream propStream = Files.newInputStream(Paths.get(filename))) {
            props.load(propStream);
        }
        return props;
    }


}
