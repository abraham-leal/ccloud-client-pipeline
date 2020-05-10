package io.confluent.ps.abraham.leal;

import events.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.errors.RetriableException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

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

public class registrationEvent {

    public static  Random getNum = new Random();
    public static Logger log = LoggerFactory.getLogger(registrationEvent.class);
    private static KafkaProducer<String, customerInformation> reusableProducer;

    public registrationEvent(KafkaProducer<String, customerInformation> x){
        reusableProducer = x;
    }

    // Production method
    // It is important to decouple production from the main method in order to be able to unit test properly
    // Much of the error handling is happening here
    public void sendRegistration (String topic, orderDetail currentPurchase) throws IOException {
        final customerInformation recordValue = getRegistration(currentPurchase);
        final ProducerRecord<String, customerInformation> record = new ProducerRecord<String, customerInformation>(topic, null, recordValue);
        try{
            reusableProducer.send(record, (recordMetadata, exception) -> {
                if (exception != null) {
                    if (exception instanceof RetriableException) {
                        // We've received a retriable exception to trying to send
                        // a record. Due to this, we will try to flush to disk.
                        // Another option may be to send the record upstream, but since we generate our records locally,
                        // this isn't really an option
                        log.warn("Production has failed after the set amount of retries. Writing registration to disk...");
                        try {
                            writeToFile(record, "ProducerRetriableError");
                        }catch (IOException i) { //Handle not being able to write to file in local directory
                            log.warn("Could not write to file in local path to handle undelivered record. Aborting.");
                            i.printStackTrace();
                        }
                    } else { // Handle Unretriable Exceptions
                        log.warn("Production to topic " + recordMetadata.topic() + " has failed unrecoverably. Ending Producer.");
                        exception.printStackTrace();
                    }
                }});
        }catch (SerializationException s){
            log.warn("Message could not be properly serialized. Flushing to disk for later retrieval in local" +
                    "directory='ProducerBadSerialization.log' ");
            writeToFile(record, "ProducerBadSerialization");
        }
    }

    public static void writeToFile (ProducerRecord<String,customerInformation> recordToWrite, String filename) throws IOException {
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

    private static customerInformation getRegistration(orderDetail currentOrder){
        customerInformation newRegistration = new customerInformation();
        List<CharSequence> orders = new ArrayList<CharSequence>();
        orders.add(Integer.toString(currentOrder.getOrderNumber()));
        newRegistration.setAddress("1234 Registration St, Austin, TX 78703");
        newRegistration.setName(getName());
        newRegistration.setLastName(getLastName());
        newRegistration.setOrders(orders);

        return newRegistration;
    }

    private static Integer getAddress (){
        return getNum.nextInt(10000 - 1000) + 1000;
    }

    private static String getName(){
        String [] x = {"Abraham", "Russell", "Bob", "Mark","Jay","Valeria", "Miguel", "Jaime"};
        return x[getNum.nextInt(x.length)];
    }

    private static String getLastName(){
        String [] x = {"Leal", "Kreps", "Rao", "Smith","Lynaugh","Tarrago"};
        return x[getNum.nextInt(x.length)];
    }
}
