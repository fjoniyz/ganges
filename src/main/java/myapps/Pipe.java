package myapps;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import java.util.*;
import java.util.stream.Collectors;

/**
 * In this example, we implement a simple Pipe program using the high-level Streams DSL that reads
 * from a source topic "streams-plaintext-input", where the values of messages represent lines of
 * text, and writes the messages as-is into a sink topic "streams-pipe-output".
 */
public class Pipe {

  public static void main(final String[] args) {
    String userDirectory = System.getProperty("user.dir");
    try (InputStream inputStream =
        Files.newInputStream(Paths.get(userDirectory + "/src/main/resources/config.properties"))) {
      Properties props = new Properties();

      String inputTopic = "input-data";
      String outputTopic = "output-events";

      props.load(inputStream);
      props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      props.put(
          StreamsConfig.METADATA_MAX_AGE_CONFIG,
          "1000"); // Needed to prevent timeouts during broker startup.

      final StreamsBuilder builder = new StreamsBuilder();
      KStream<String, String> src = builder.stream(inputTopic);
      src.mapValues(
              value -> {
                String[] values = value.split(",");
                String[] parsed = {values[values.length - 1]};
                System.out.println("Values: " + Arrays.toString(parsed));
                List<String> strings = Arrays.asList(values);
                List<Double> listOfDoubles =
                    strings.stream().map(Double::valueOf).collect(Collectors.toList());
                double[] arr = listOfDoubles.stream().mapToDouble(Double::doubleValue).toArray();
                double[][] input = new double[][] {arr, arr};
                System.out.println("Input: " + input);
                double[][] output = Doca.doca(input, 0.01, 1000, 60, 100, false);
                System.out.println("Output: " + output);
                String result = Arrays.deepToString(output);
                System.out.println("Result: " + result);
                return result;
              })
          .to(outputTopic);

      final Topology topology = builder.build();
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
