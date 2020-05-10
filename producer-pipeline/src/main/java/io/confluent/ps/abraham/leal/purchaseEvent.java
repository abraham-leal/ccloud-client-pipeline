package io.confluent.ps.abraham.leal;

import events.orderDetail;
import kafka.Kafka;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class purchaseEvent implements Runnable{

    public static  Random getNum = new Random();
    private Properties securityConfiguration;
    private KafkaProducer reusableProducer;
    public static Logger log = LoggerFactory.getLogger(purchaseEvent.class);

    public purchaseEvent (KafkaProducer reusable, Properties securityConfigurations){
        this.securityConfiguration = securityConfigurations;
        this.reusableProducer = reusable;
    }

    // Production method
    // It is important to decouple production from the main method in order to be able to unit test properly
    // Much of the error handling is happening here
    public static void produce (KafkaProducer<String, orderDetail> producer, String topic, orderDetail order) throws IOException {
        final ProducerRecord<String, orderDetail> record = new ProducerRecord<String, orderDetail>(topic, null, order);
        try{

            producer.send(record, (recordMetadata, exception) -> {
                if (exception != null) {
                    if (exception instanceof RetriableException) {
                        // We've received a retriable exception to trying to send
                        // a record. Due to this, we will try to Flush to disk for later pick up by a scraper
                        // Another option may be to send the record upstream, but since we generate our records locally,
                        // this isn't really an option
                        log.warn("Production has failed after the set amount of retries. Flushing purchase to local directory...");
                        try {
                            writeToFile(record, "ProducerRetriableError");
                            exception.printStackTrace();
                        } catch (IOException i) { //Handle not being able to write to file in local directory
                            log.warn("Could not write to file in local path to handle undelivered record. Aborting.");
                            i.printStackTrace();
                        }

                    } else { // Handle Unretriable Exceptions
                        log.warn("Production to topic " + recordMetadata.topic() + "Has failed unrecoverably. Ending Producer.");
                        exception.printStackTrace();
                    }
                }});
        }catch (SerializationException s){
            log.warn("Message could not be properly serialized. Flushing to disk for later retrieval in local" +
                    "directory='ProducerLog.log' ");
            writeToFile(record, "ProducerBadSerialization");
        }
    }

    public static void writeToFile (ProducerRecord<String,orderDetail> recordToWrite, String filename) throws IOException {
        // This method appends unsent records to a file in the local directory
        File unsentLog = new File(filename +".log");
        if (unsentLog.exists()) {
            Files.write(Paths.get(filename+".log"), recordToWrite.toString().getBytes(), StandardOpenOption.APPEND);
        }else{
            unsentLog.createNewFile();
            Files.write(Paths.get(filename+".log"), recordToWrite.toString().getBytes(), StandardOpenOption.APPEND);
        }
    }


    // Helper methods to generate fake data.

    // Method to aleatorily pick whether a specific purchase is a registration of a new user.
    // We'll be generating many purchase events, so we want this to be a rare occurrence.
    // Overall, only 1% of our purchases will be people registering along.
    // We do this by passing a probability 0 < p < 1 that we get true
    private static boolean isRegistration(float prob){
        return getNum.nextFloat() < prob;
    }

    public static Integer getID (){
        return getNum.nextInt(10000 - 1000) + 1000;
    }

    private static String getCurrency(){
        String [] currencies = {"USD","MXN","GBP","EUR","JPY","CHF"};
        return currencies[getNum.nextInt(currencies.length)];
    }

    private static String getItem(){
        String [] x = {"Lumber", "Window", "Paint", "Chair","Stair","Drill", "Hammer", "Pot"};
        return x[getNum.nextInt(x.length)];
    }

    // A ON will be a random 10 digit number
    private static Integer getON(){
        return getNum.nextInt(100000000) + 1000000000;
    }

    private static Integer getOverallOrderNumber (){
        return getNum.nextInt(99) + 100;
    }

    // Date in form of epoch
    private static Long getDate(){ return System.currentTimeMillis();}

    public static orderDetail buildPurchase(){
        orderDetail newOrder = new orderDetail();
        newOrder.setCurrency(getCurrency());
        newOrder.setItem(getItem());
        newOrder.setOrderDate(getDate());
        newOrder.setOrderNumber(getON());
        newOrder.setPartOfOrder(getOverallOrderNumber());

        return newOrder;
    }


    @Override
    public void run() {
        registrationEvent registrations = new registrationEvent(reusableProducer);
        String topic = "purchaseEvents";

        //Production Loop
        while(true) { // Passing generated data by helper methods to the producing method
            orderDetail currentPurchase = buildPurchase();
            if (isRegistration(0.01f)){
                try {
                    registrations.sendRegistration("registrationEvents", currentPurchase);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                produce(reusableProducer, topic, currentPurchase);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
