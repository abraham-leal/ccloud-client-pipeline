package io.confluent.ps.abraham.leal.processors;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.confluent.ps.ServingRecord;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import javax.validation.constraints.Null;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class getFromREST implements ValueTransformerWithKey<String, ServingRecord,ServingRecord> {
    // This is an example of a transformer that queries a rest service and
    // returns a new value with the result


    @Override
    public void init(ProcessorContext context) {

    }

    @Override
    public ServingRecord transform(String key, ServingRecord value) {
        ServingRecord newValue = value;
        try {
            newValue.setRemainingFillCount(Integer.getInteger(getValue("APPL")));
            newValue.setRefillable(!getValue("GOOGL").isEmpty());
            newValue.setNinetyFillFlag(String.valueOf(!getValue("FB").isEmpty()));
            newValue.setAutoRefillInd(String.valueOf(!getValue("AAPL").isEmpty()));
        } catch (NullPointerException | IOException e) {
            System.out.println("REST Service is unreachable");
            e.printStackTrace();
        }


        return newValue;
    }

    private static String getValue(String restService) throws IOException {
        CloseableHttpClient client = HttpClientBuilder.create().build();

        final String service1 = "APPL";
        final String service2 = "GOOGL";
        final String service3 = "FB";

        HttpGet request = null;

        switch (restService){
            case service1:
                String endpoint = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=AAPL&apikey=RZ1EGK8UMQMA4UYD";
                request = new HttpGet(endpoint);
                break;
            case service2:
                endpoint = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=GOOGL&apikey=RZ1EGK8UMQMA4UYD";
                request = new HttpGet(endpoint);
                break;
            case service3:
                endpoint = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=FB&apikey=RZ1EGK8UMQMA4UYD";
                request = new HttpGet(endpoint);
                break;
            default:
                System.out.println("Non supported REST Service");
                return null;
        }

        HttpResponse getData = client.execute(request);
        BufferedReader reader = new BufferedReader (new InputStreamReader(getData.getEntity().getContent()));
        String JSONString = org.apache.commons.io.IOUtils.toString(reader);

        JsonParser parser =  new JsonParser();
        JsonElement jsonElement = parser.parse(JSONString);
        JsonObject jsonObj = jsonElement.getAsJsonObject();

        return jsonObj.getAsJsonObject("Global Quote").getAsJsonPrimitive("05. price").getAsString();

    }


    @Override
    public void close() {

    }
}
