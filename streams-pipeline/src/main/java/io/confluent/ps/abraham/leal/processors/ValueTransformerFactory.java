package io.confluent.ps.abraham.leal.processors;

import io.confluent.ps.ServingRecord;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;

public class ValueTransformerFactory implements ValueTransformerWithKeySupplier<String, ServingRecord,ServingRecord> {
    @Override
    public ValueTransformerWithKey<String,ServingRecord, ServingRecord> get() {
        return new getFromREST();
    }
}
