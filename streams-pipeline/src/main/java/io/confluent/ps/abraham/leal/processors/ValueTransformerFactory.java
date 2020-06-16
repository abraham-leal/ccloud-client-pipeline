package io.confluent.ps.abraham.leal.processors;

import io.confluent.gen.Tbf0RxTransaction;
import io.confluent.ps.ServingRecord;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;

public class ValueTransformerFactory implements ValueTransformerWithKeySupplier<String, Tbf0RxTransaction,Tbf0RxTransaction> {
    @Override
    public ValueTransformerWithKey<String, Tbf0RxTransaction, Tbf0RxTransaction> get() {
        return new getFromMongo();
    }
}
