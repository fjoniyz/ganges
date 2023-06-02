package myapps;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;

/**
 * In this example, we implement a simple Pipe program using the high-level
 * Streams DSL that reads
 * from a source topic "streams-plaintext-input", where the values of messages
 * represent lines of
 * text, and writes the messages as-is into a sink topic "streams-pipe-output".
 */
public class Pipe {

  public static void main(final String[] args) {
    String inputTopic = EnvTools.getEnvValue(EnvTools.INPUT_TOPIC, "streams-input");
    String outputTopic = EnvTools.getEnvValue(EnvTools.OUTPUT_TOPIC, "streams-output");
    String bootstrapServerConfig = EnvTools.getEnvValue(EnvTools.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    String jsonStringConfig = EnvTools.getEnvValue(EnvTools.ANON_CONFIG, "{\n"
        + " \"functionName\": \"delta-doca\", \n"
        + "  \"parameters\": { \n"
        + "    \"eps\": 0.01, \n"
        + "    \"delay_constraint\": 1000 \n"
        + "    \"beta\": 60, \n"
        + "    \"mi\": 100, \n"
        + "    \"inplace\": false \n"
        + "  }"
        + "}"); // Read the configuration form environment variable
    final JSONObject config = new JSONObject(jsonStringConfig);

    AnonymizationFunction anonymize = null;
    String functionName = config.get("functionName").toString();

    // TODO: Add correct parameters for each function
    if (functionName.equals("delta-doca")) {
      anonymize = (input, parameters) -> {
        int eps = parameters.getInt("eps");
        int delay_constraint = parameters.getInt("delay_constraint");
        int beta = parameters.getInt("beta");
        int mi = parameters.getInt("mi");
        boolean inplace = parameters.getBoolean("inplace");
        return Doca.doca(input, eps, delay_constraint, beta, mi, inplace);
      };
    } else if (functionName.equals("castle-guard")) {
      anonymize = (input, parameters) -> {
        int eps = parameters.getInt("eps");
        int delay_constraint = parameters.getInt("delay_constraint");
        int beta = parameters.getInt("beta");
        int mi = parameters.getInt("mi");
        boolean inplace = parameters.getBoolean("inplace");
        return CastleGuard.castle_guard(input, eps, delay_constraint, beta, mi, inplace);
      };
    }

    // Create the input and output topics with the broker
    createTopics(bootstrapServerConfig, inputTopic, outputTopic);

    // Set up the streams configuration.
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerConfig);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(
        StreamsConfig.METADATA_MAX_AGE_CONFIG,
        "1000"); // Needed to prevent timeouts during broker startup.

    // Create an instance of StreamsBuilder
    final StreamsBuilder builder = new StreamsBuilder();

    // Read the input Kafka topic into a KStream instance
    KStream<String, String> inputStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

    // Apply the anonymization function to the input stream
    KStream<String, String> outputStream = inputStream
        .mapValues(value -> anonymize.execute(value, config.getJSONObject("parameters")));

    // Write the output stream to the output topic
    outputStream.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

    // Build the topology and start streaming!
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
  };

  private static void createTopics(String bootstrapServerConfig, String inputTopic, String outputTopic) {
    // Create an AdminClient to create the input and output topics with the broker
    Properties adminProps = new Properties();

    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerConfig);

    AdminClient adminClient = AdminClient.create(adminProps);

    // Create the input and output topics
    // with 1 partition and a replication factor of 1
    NewTopic newInputTopic = new NewTopic(inputTopic, 1, (short) 1);
    NewTopic newOutputTopic = new NewTopic(outputTopic, 1, (short) 1);

    adminClient.createTopics(Collections.singleton(newInputTopic));
    adminClient.createTopics(Collections.singleton(newOutputTopic));

    adminClient.close();
  }
};
