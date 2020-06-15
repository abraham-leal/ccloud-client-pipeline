package io.confluent.ps.abraham.leal;

import io.confluent.gen.ServingRecord;
import io.confluent.gen.Tbf0Prescriber;
import io.confluent.gen.Tbf0Rx;
import io.confluent.gen.Tbf0RxTransaction;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class runHydration {
    final static Logger logger = Logger.getRootLogger();
    final static String BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
    final static String SR_BOOTSTRAP_SERVERS = System.getenv("SR_BOOTSTRAP_SERVERS");
    final static String API_KEY = System.getenv("API_KEY");
    final static String API_SECRET = System.getenv("API_SECRET");
    final static String SR_API_KEY = System.getenv("SR_API_KEY");
    final static String SR_API_SECRET = System.getenv("SR_API_SECRET");
    final static String JAAS_CONF = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", API_KEY,API_SECRET);
    public static Properties getConfig (){

        final Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "getHydrated");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde.class);
        streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        streamsProps.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "3");
        streamsProps.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG,"SASL_SSL");
        streamsProps.put(SaslConfigs.SASL_MECHANISM,"PLAIN");
        streamsProps.put(SaslConfigs.SASL_JAAS_CONFIG,JAAS_CONF);
        streamsProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,SR_BOOTSTRAP_SERVERS);
        streamsProps.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG,SR_API_KEY+":"+SR_API_SECRET);
        streamsProps.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO");

        try {
            Map<String,String> sysEnvs = System.getenv();
            for (String env : sysEnvs.keySet()){
                if (env.startsWith("KSTREAMS_")){
                    logger.info("Importing config: " + env);
                    streamsProps.put(env.replaceFirst("KSTREAMS_","").replace('_','.'),sysEnvs.get(env));
                }
            }
        }catch (Exception e){
            logger.info("Could not import custom variable to the KStreams Application");
            e.printStackTrace();
        }

        return streamsProps;
    }

    public static Serde getSerdeConfigs (String className){
        final Serde<ServingRecord> serveSerde = new KafkaJsonSchemaSerde<>();
        final Serde<Tbf0Prescriber> presSerde = new KafkaJsonSchemaSerde<>();
        final Serde<Tbf0Rx> rxSerde = new KafkaJsonSchemaSerde<>();
        final Serde<Tbf0RxTransaction> rxtSerde = new KafkaJsonSchemaSerde<>();

        Map<String, Object> SRConfig = new HashMap<>();
        SRConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,SR_BOOTSTRAP_SERVERS);
        SRConfig.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG,SR_API_KEY+":"+SR_API_SECRET);
        SRConfig.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO");

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

    public static void main(String[] args) {
        BasicConfigurator.configure();
        logger.setLevel(Level.INFO);

        // Set up custom serdes to configure topics with
        final Serde<ServingRecord> serveSerde = getSerdeConfigs("serve");
        final Serde<Tbf0Prescriber> presSerde = getSerdeConfigs("pres");
        final Serde<Tbf0Rx> rxSerde = getSerdeConfigs("rx");
        final Serde<Tbf0RxTransaction> rxtSerde = getSerdeConfigs("trans");

        KafkaStreams app = new KafkaStreams(completeRecordJSON.getTopology(serveSerde,presSerde,rxSerde,rxtSerde).build(),getConfig());
        try {
            logger.info("Starting Kafka Streams Application");
            app.start();
        }catch (Exception e){
            logger.info("Unable to start Kafka Streams Application");
        }
        Runtime.getRuntime().addShutdownHook(new Thread(app::close));
    }
}
