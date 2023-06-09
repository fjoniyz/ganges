package myapps;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.*;

import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * In this example, we implement a simple Pipe program using the high-level Streams DSL that reads
 * from a source topic "streams-plaintext-input", where the values of messages represent lines of
 * text, and writes the messages as-is into a sink topic "streams-pipe-output".
 */
public class Pipe {

    public static void main(final String[] args) {
        String userDirectory = System.getProperty("user.dir");
        try (InputStream inputStream = Files.newInputStream(Paths.get(userDirectory + "/src/main/resources/config.properties"))) {
            Properties props = new Properties();

            String inputTopic = "input-test";
            String outputTopic = "output-test";
            String storeTopic = "store-topic";

            props.load(inputStream);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, "1000"); // Needed to prevent timeouts during broker startup.

            final StreamsBuilder streamsBuilder = new StreamsBuilder();
            KStream<String, String> src = streamsBuilder.stream(inputTopic);
//            src.to(storeTopic);

            StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("values"), Serdes.String(), Serdes.String());
            streamsBuilder.addGlobalStore(storeBuilder, outputTopic, Consumed.with(Serdes.String(), Serdes.String()), () -> new ProcessorImpl("values"));
            src.process(() -> new ProcessorImpl("values"));
            Topology topology = streamsBuilder.build();
            final KStream<String, String> finalStream = src.transformValues(() -> new ValueTransformer<String, String>() {
                KeyValueStore<String, String> store;

                @Override
                public void init(ProcessorContext context) {
                    this.store = context.getStateStore("values");
                }

                @Override
                public String transform(String value) {
                    String parsedValue = value.substring(1, value.length() - 1);
                    System.out.println("Parsed values: " + parsedValue);
                    System.out.println("Store: " + store);
                    String[] values = parsedValue.split(",");
                    String[] parsed = {values[values.length - 1]};
                    System.out.println("Values: " + Arrays.toString(parsed));
                    List<String> strings = Arrays.asList(values);
                    List<Double> listOfDoubles = strings.stream().map(Double::valueOf).collect(Collectors.toList());
                    double[] arr = listOfDoubles.stream().mapToDouble(Double::doubleValue).toArray();
                    double[][] input = new double[][]{arr, arr};
                    System.out.println("Input: " + input);
                    double[][] output = Doca.doca(input, 100, 1000, 60, false);
                    System.out.println("Output: " + output);
                    String result = Arrays.deepToString(output);
                    System.out.println("Result: " + result);
                    return result;
                }

                @Override
                public void close() {
                    // No-op
                }
            });
            finalStream.to(outputTopic);
            src.process(() -> new ProcessorImpl("values"), "values");

            try (KafkaStreams streams = new KafkaStreams(topology, props)) {
                final CountDownLatch latch = new CountDownLatch(1);

                try {
                    streams.start();
                    latch.await();
                } catch (Throwable e) {
                    System.exit(1);
                }
            }
            System.exit(0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
