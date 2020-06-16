package io.confluent.ps.abraham.leal.processors;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.confluent.gen.Tbf0RxTransaction;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.log4j.Logger;
import org.bson.*;

import java.util.Timer;

public class getFromMongo implements ValueTransformerWithKey<String, Tbf0RxTransaction,Tbf0RxTransaction> {
    // This is an example of a transformer that queries mongoDB and
    // returns a new value with the processed result
    private MongoClient mongoClient;
    private MongoDatabase mongoDb;
    private final Logger logger = Logger.getLogger(getFromMongo.class);
    private final String connectionString = System.getenv("MongoConnectionString");
    private final String dbName = System.getenv("MongoDBName");
    private final String collection = System.getenv("MongoCollection");
    @Override
    public void init(ProcessorContext context) {
        // Initialize MongoDB client
        logger.info("Opened connection to MongoDB");
        mongoClient = MongoClients.create(connectionString);
        // Get connection to specific DB, this assumes info from a single DB,
        // but multiple DB clients can be instantiated
        logger.info("Got database with name: " + dbName);
        mongoDb = mongoClient.getDatabase(dbName);
    }

    @Override
    public Tbf0RxTransaction transform(String key, Tbf0RxTransaction value) {
        // Extract needed key from record and construct filter document
        BsonDocument filterFields = BsonDocument.parse(key);
        BsonDocument filterDocument = new BsonDocument("_id",filterFields);

        // get Document from MongoDB
        Document lookupResult =  mongoDb.getCollection(collection).find(filterDocument).first();

        // Do whatever you'd like with the result
        // This is just an example
        if(lookupResult!=null){
            logger.debug("Document successfully obtained");
            value.setFILLSOURCECD(lookupResult.get("RX_ORIGINAL_QTY_DISP").toString());
        } else {
            // Here handle the case where no document is found
            // This will throw an error and stop the application
            throw new BsonInvalidOperationException("No Document Found with given key");
        }

        // Return modified value for further processing
        return value;
    }


    @Override
    public void close() {
        // Close out mongo connection when finished processing
        mongoClient.close();
    }
}
