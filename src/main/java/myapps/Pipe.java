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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.streams.kstream.Consumed;

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

    // Create an AdminClient to creat the input and output topics
    Properties adminProps = new Properties();
    
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerConfig);

    // Create an AdminClient
    AdminClient adminClient = AdminClient.create(adminProps);

    // Create the input and output topics with 1 partition and a replication factor of 1
    NewTopic newInputTopic = new NewTopic(inputTopic, 1, (short) 1);
    NewTopic newOutputTopic = new NewTopic(outputTopic, 1, (short) 1);

    // Create the topics using the AdminClient
    adminClient.createTopics(Collections.singleton(newInputTopic));
    adminClient.createTopics(Collections.singleton(newOutputTopic));

    // Close the AdminClient
    adminClient.close();

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

    // Read the input Kafka topic into a KStream instance and 
    // process the stream by applying teh modifyJson method to each value
    // write the result to the output topic
    builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String())).mapValues(Pipe::modifyJson)
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
  }

  private static String modifyJson(String json) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode jsonNode = objectMapper.readTree(json);
      ((ObjectNode) jsonNode).put("AEP_MW", "x");

      return jsonNode.toString();
    } catch (Exception e) {
      // Handle any exception that occurred during JSON processing
      e.printStackTrace();
      return null; // or throw an exception if desired
    }
  }
}
