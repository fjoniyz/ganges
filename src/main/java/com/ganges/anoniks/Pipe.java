package com.ganges.anoniks;

import com.ganges.benchmark.LocalMetricsCollector;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ganges.anonlib.AnonymizationAlgorithm;
import com.ganges.anonlib.AnonymizationItem;
import com.ganges.anonlib.DataRepository;
import com.ganges.anonlib.castleguard.CastleGuard;
import com.ganges.anonlib.doca.Doca;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import com.ganges.serdes.Deserializer;
import com.ganges.serdes.Serializer;

public class Pipe {

  private static final String CASTLEGUARD_KEY = "castleguard";
  private static final String DOCA_KEY = "doca";
  private static final String DOCA_ALT_KEY = "doca-alt";
  private static AnonymizationAlgorithm algorithm;
  private static Properties props = new Properties();

  private static LocalMetricsCollector metricsCollector;

  /**
   * This method extracts values from a JSON message based on keys specified in the
   * configuration file.
   * Each specified key is used to retrieve a corresponding numeric value from the JSON message.
   * If a key is not present in the JSON message, an exception is thrown.
   *
   * @param jsonNode The root node of the incoming JSON message.
   * @param fields   A list of keys representing the fields to extract from the JSON message.
   * @return A mapping of field names to their associated numeric values from the JSON message.
   * @throws IllegalArgumentException If a key is not present in the JSON message.
   */
  private static HashMap<String, Double> getValuesListByKeys(JsonNode jsonNode,
                                                             List<String> fields) {
    HashMap<String, Double> values = new HashMap<>();
    for (String field : fields) {
      JsonNode value = jsonNode.get(field);
      if (value != null) {
        values.put(field, value.doubleValue());
      } else {
        throw new IllegalArgumentException("Invalid field in config file: " + field);
      }
    }
    return values;
  }

  /**
   * This method extracts values from a JSON message for the fields that are not marked for
   * anonymization.
   * Fields specified by anonFields are excluded from the output.
   *
   * @param jsonNode   The root node of the incoming JSON message.
   * @param anonFields A list of fields representing the headers to be anonymized.
   * @return A mapping of header names to their corresponding non-anonymized values
   *                     from the JSON message.
   */
  private static HashMap<String, String> getNonAnonymizedValuesByKeys(JsonNode jsonNode,
                                                                      List<String> anonFields) {
    HashMap<String, String> values = new HashMap<>();
    Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> jsonEntry = it.next();
      if (!anonFields.contains(jsonEntry.getKey())) {
        if (jsonEntry.getValue().isNumber()) {
          values.put(jsonEntry.getKey(), jsonEntry.getValue().toString());
        } else {
          values.put(jsonEntry.getKey(), jsonEntry.getValue().textValue());
        }
      }
    }
    return values;
  }

  /**
   * Processes a JSON message for anonymization and generates an output JSON message with
   * anonymized data.
   *
   * @param message                The input JSON message to be processed and anonymized. Must have
   *                               flat structure (no nesting)
   * @param dataRepository         The redis repository for retrieving context information needed
   *                               for anonymization.
   * @param anonFields             An array of field names representing the data fields
   *                               to be anonymized.
   * @param addUnanonymizedHistory A flag indicating whether to add unanonymized data history to
   *                               the context values.
   * @return An output JSON message containing anonymized data.
   * @throws IOException If an I/O error occurs during JSON conversion or data repository access.
   */
  private static JsonNode processing(JsonNode message, DataRepository dataRepository,
                                     String[] anonFields,
                                     boolean addUnanonymizedHistory, boolean enableMetrics) throws IOException {


    // Get values of current message
    System.out.println("message: " + message);
    String id = message.get("ae_session_id").textValue();
    if (enableMetrics) {
      metricsCollector.setProducerTimestamps(message);
      metricsCollector.setPipeEntryTimestamps(message.get("ae_session_id").textValue(),
          System.currentTimeMillis());
    }

    ObjectMapper mapper = new ObjectMapper();
    Map<String, String> messageMap = mapper.convertValue(message, new TypeReference<>() {
    });

    List<String> anonFieldsList = List.of(anonFields);
    HashMap<String, Double> valuesMap = getValuesListByKeys(message, anonFieldsList);
    HashMap<String, String> nonAnonymizedValuesMap = getNonAnonymizedValuesByKeys(message,
        anonFieldsList);
    System.out.println(message);

    // Get all entries needed for anonymization
    List<AnonymizationItem> contextValues = new ArrayList<>();
    dataRepository.open();
    // Retrieve non-anonymized data from cache
    dataRepository.saveValues(messageMap);

    String[] weights = props.getProperty("prioritization").split(",");
    Map<String, Double> headerWeights = new HashMap<>();
    for (int i = 0; i < weights.length; i++) {
      headerWeights.put(anonFields[i], Double.parseDouble(weights[i]));
    }

    if (addUnanonymizedHistory) {

      // Here we assume that for one message type the values are stored consistently (all
      // fields are present in cache)
      contextValues = dataRepository.getValuesByKeys(anonFieldsList, headerWeights);
    } else {

      contextValues.add(new AnonymizationItem(id, valuesMap, nonAnonymizedValuesMap, headerWeights));
    }
    dataRepository.close();

    // Anonymization
    if (enableMetrics) {
      metricsCollector.setAnonEntryTimestamps(id,
          System.currentTimeMillis());
    }
    List<AnonymizationItem> output = algorithm.anonymize(contextValues);
    if (enableMetrics) {
      long anonExitTimestamp = System.currentTimeMillis();
      for (AnonymizationItem item : output) {
        metricsCollector.setAnonExitTimestamps(item.getId(), anonExitTimestamp);
      }
    }

    ArrayNode outputMessage = getJsonFromItems(output);

    if (enableMetrics) {
      long pipeExitTimestamp = System.currentTimeMillis();
      for (AnonymizationItem item : output) {
        metricsCollector.setPipeExitTimestamps(item.getId(), pipeExitTimestamp);
      }
      if (!output.isEmpty()) {
        metricsCollector.sendCurrentResultsToRemote();
      }
    }

    System.out.println("OUTPUT " + outputMessage);
    return outputMessage;
  }

  /**
   * Converts a list of AnonymizationItem objects into a JSON array containing anonymized data.
   *
   * @param outputItems A list of AnonymizationItem objects representing processed records
   *                    with anonymized data.
   * @return A JSON array containing anonymized data for each item in the provided list.
   */
  private static ArrayNode getJsonFromItems(List<AnonymizationItem> outputItems) {
    ObjectMapper mapper = new ObjectMapper();
    ArrayNode outputMessage = mapper.createArrayNode();
    for (AnonymizationItem item : outputItems) {
      ObjectNode itemNode = mapper.createObjectNode();
      itemNode.put("id", item.getId());

      // Add anonymized values
      Map<String, Double> itemValues = item.getValues();
      for (String key : itemValues.keySet()) {
        itemNode.put(key, itemValues.get(
            key).floatValue());
      }

      // Add non-anonymized values
      for (Map.Entry<String, String> entry : item.getNonAnonymizedValues().entrySet()) {
        itemNode.put(entry.getKey(), entry.getValue());
      }
      outputMessage.add(itemNode);
    }
    return outputMessage;
  }

  /**
   * Retrieves an array of fields to be anonymized based on configuration file pipe.properties.
   *
   * @return An array of field names to be anonymized.
   * @throws IOException If an I/O error occurs while reading the configuration file.
   */
  public static String[] getFieldsToAnonymize() throws IOException {
    String userDirectory = System.getProperty("user.dir");
    try (InputStream inputStream = Files.newInputStream(
        Paths.get(userDirectory + "/src/main/resources/anoniks/pipe.properties"))) {
      Properties properties = new Properties();
      properties.load(inputStream);
      String fieldsStr = properties.getProperty("anonymized_fields");
      String[] anonFields = fieldsStr.split(",");
      System.out.println("Fields to be anonymized: " + String.join(" ", anonFields));
      return anonFields;
    }
  }

  public static void main(final String[] args)
      throws IOException, ExecutionException, InterruptedException {
    String userDirectory = System.getProperty("user.dir");

    metricsCollector = LocalMetricsCollector.getInstance();
    System.out.println("Created local metrics collector");

    try (InputStream inputStream = Files.newInputStream(
        Paths.get(userDirectory + "/src/main/resources/anoniks/pipe.properties"))) {
      props.load(inputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    String inputTopic = props.getProperty("input-topic");
    String outputTopic = props.getProperty("output-topic");
    String anonymizationAlgoName = props.getProperty("algorithm");
    boolean enableMetrics = Boolean.parseBoolean(props.getProperty("enable-metrics"));

    boolean addUnanonymizedHistory = anonymizationAlgoName.equals(DOCA_ALT_KEY);
    if (anonymizationAlgoName.equals(CASTLEGUARD_KEY)) {
      algorithm = new CastleGuard();
    } else if (anonymizationAlgoName.equals(DOCA_KEY)) {
      algorithm = new Doca();
    } else if (anonymizationAlgoName.equals(DOCA_ALT_KEY)) {
      algorithm = new Doca();
    } else {
      throw new IllegalArgumentException("Unknown anonymization algorithm passed");
    }
    Serializer<JsonNode> serializer = new Serializer<>();
    Deserializer deserializer = new Deserializer();
    Serde<JsonNode> jsonSerde = Serdes.serdeFrom(serializer, deserializer);
    try (InputStream inputStream = Files.newInputStream(
        Paths.get(userDirectory + "/src/main/resources/anoniks/kafka.properties"))) {
      props.load(inputStream);
      props.put(StreamsConfig.METADATA_MAX_AGE_CONFIG,
          "1000"); // Needed to prevent timeouts during broker startup.
      props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, jsonSerde.getClass());

      // Create anonymization stream and use it with Kafka
      StreamsBuilder streamsBuilder = new StreamsBuilder();
      KStream<String, JsonNode> src =
          streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), jsonSerde));
      Produced<String, JsonNode> produced = Produced.with(Serdes.String(), jsonSerde);

      DataRepository dataRepository = new DataRepository();
      String[] fields = getFieldsToAnonymize();
      src.mapValues(value -> {
        try {
          return processing(value, dataRepository, fields, addUnanonymizedHistory, enableMetrics);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }).to(outputTopic, produced);
      Topology topology = streamsBuilder.build();

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
