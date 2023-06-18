package myapps;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.ganges.lib.castleguard.CGConfig;
import com.ganges.lib.castleguard.CastleGuard;
import com.ganges.lib.castleguard.Item;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

/**
 * In this example, we implement a simple Pipe program using the high-level Streams DSL that reads
 * from a source topic "streams-plaintext-input", where the values of messages represent lines of
 * text, and writes the messages as-is into a sink topic "streams-pipe-output".
 */
public class Pipe {

  public static void main(final String[] args) {
    Properties props = new Properties();
    String inputTopic = EnvTools.getEnvValue(EnvTools.INPUT_TOPIC, "streams-input");
    String outputTopic = EnvTools.getEnvValue(EnvTools.OUTPUT_TOPIC, "streams-output");
    String bootstrapServerConfig =
        EnvTools.getEnvValue(EnvTools.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerConfig);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(
        StreamsConfig.METADATA_MAX_AGE_CONFIG,
        "1000"); // Needed to prevent timeouts during broker startup.

    final StreamsBuilder builder = new StreamsBuilder();
    CGConfig config = new CGConfig(2, 2, 5, 1, 5, 1, 70, true);
    Deque<Item> items = new ArrayDeque<>();

    // Reading data using readLine
    List<String> headers = new ArrayList<>(List.of(new String[] {"sensitive", "attr1", "attr2"}));

    CastleGuard anonymization = new CastleGuard(config, headers, headers.get(0));
    KStream<String, String> src = builder.stream(inputTopic);

    src.mapValues(value -> {
      List<Float> values = Stream.of(value.split(","))
              .map(Float::parseFloat)
              .collect(Collectors.toList());
      HashMap<String, Float> rowData = new HashMap<>();
      for (int i = 0; i < values.size(); i++) {
        rowData.put(headers.get(i), values.get(i));
      }
      anonymization.insertData(rowData);
      Optional<HashMap<String, Float>> anonData = anonymization.tryGetOutputLine();
      List<String> result = new ArrayList<>();
      if (anonData.isPresent()) {
        System.out.print("Anon data: ");
        anonData.get().entrySet().forEach(entry -> result.add(entry.getKey() + ": " + entry.getValue() + " "));
        System.out.println();
      }
      System.out.printf("Result: " + String.join(", ", result));
      return String.join(", ", result);
    }).to(outputTopic);

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
  }
}
