package io.confluent.ps.abraham.leal;

public class completeRecord {

    public static Properties getConfig (){
        final Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "EnchanceRecord");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "<CCLOUD_DNS>");
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        streamsProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"<CCLOUD_SR_DNS>");
        streamsProps.put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG,"<CCLOUD_SR_KEY>:<CCLOUD_SR_SECRET>");
        streamsProps.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO");
        streamsProps.put("security.protocol","SASL_SSL");
        streamsProps.put("sasl.mechanism","PLAIN");
        streamsProps.put("ssl.endpoint.identification.algorithm","https");
        streamsProps.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule " +
                "required username=\"<API_KEY>\" " +
                "password=\"<API_SECRET>\";");

        return streamsProps;
    }

    public static void main(String[] args) {

    }
}