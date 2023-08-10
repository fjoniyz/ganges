package anoniks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ganges.lib.AnonymizationAlgorithm;
import com.ganges.lib.AnonymizationItem;
import com.ganges.lib.DataRepository;
import com.ganges.lib.castleguard.CastleGuard;
import com.ganges.lib.doca.Doca;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import serdes.Deserializer;
import serdes.Serializer;

public class Pipe {

    private final static String CASTLEGUARD_KEY = "castleguard";
    private final static String DOCA_KEY = "doca";
    private static AnonymizationAlgorithm algorithm;

    public static HashMap<String, Double> getValuesListByKeys(JsonNode jsonNode, String[] fields){
        HashMap<String, Double> values = new HashMap<>();
        for (String field : fields) {
            JsonNode value = jsonNode.get(field);
            if (value != null) {
                values.put(field,value.doubleValue());
            } else {
                throw new IllegalArgumentException("Invalid field in config file: " + field);
            }
        }
        return values;
    }

    public static HashMap<String, String> getNonAnonymizedValuesByKeys(
        JsonNode jsonNode, String[] fields) {
        HashMap<String, String> values = new HashMap<>();
        List<String> fieldsList = Arrays.asList(fields);
        Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> jsonEntry = it.next();
            if (!fieldsList.contains(jsonEntry.getKey())) {
                values.put(jsonEntry.getKey(), jsonEntry.getValue().textValue());
            }
        }
        return values;
    }


    public static JsonNode processing(JsonNode message, DataRepository dataRepository, String[] fields, boolean addUnanonymizedHistory) throws IOException {
      ObjectMapper mapper = new ObjectMapper();

      // Current message
      String id = message.get("id").textValue();
      HashMap<String, Double> valuesMap = getValuesListByKeys(message, fields);
      HashMap<String, String> nonAnonymizedValuesMap = getNonAnonymizedValuesByKeys(message,
          fields);
      System.out.println(message);


      List<AnonymizationItem> contextValues = new ArrayList<>();
      if (addUnanonymizedHistory) {
        dataRepository.open();

        // Retrieve non-anonymized data from cache
        dataRepository.saveValues(valuesMap);

        // Here we assume that for one message type the values are stored consistently (all
        // fields are present in cache)
        contextValues = dataRepository.getValuesByKeys(fields);
        dataRepository.close();
      } else {
        contextValues.add(new AnonymizationItem(id, valuesMap, nonAnonymizedValuesMap));
      }

      // Anonymization
      List<AnonymizationItem> output = algorithm.anonymize(contextValues);
      ArrayNode outputMessage = mapper.createArrayNode();

      // TODO: handle headers that start with 'spc'
      for (AnonymizationItem item : output) {
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
      System.out.println("OUTPUT " + outputMessage);
      return outputMessage;
    }

    public static String[] getFieldsToAnonymize() throws IOException {
        String userDirectory = System.getProperty("user.dir");
        try (InputStream inputStream = Files.newInputStream(Paths.get(userDirectory + "/src/main/resources/doca.properties"))) {
            Properties properties = new Properties();
            properties.load(inputStream);
            String docaFieldsString = properties.getProperty("doca_fields");
            String[] docaFields = docaFieldsString.split(",");
            for (String field : docaFields) {
                System.out.println(field);
            }
            return docaFields;
        }
    }

    public static void main(final String[] args) {
        String userDirectory = System.getProperty("user.dir");
        Properties props = new Properties();

        try (InputStream inputStream = Files.newInputStream(Paths.get(userDirectory + "/src/main/resources/pipe.properties"))) {
          props.load(inputStream);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        String inputTopic = props.getProperty("input-topic");
        String outputTopic = props.getProperty("output-topic");
        String anonymizationAlgoName = props.getProperty("algorithm");

        boolean addUnanonymizedHistory = anonymizationAlgoName.equals(DOCA_KEY);
        if (anonymizationAlgoName.equals(CASTLEGUARD_KEY)) {
          algorithm = new CastleGuard();
        } else if (anonymizationAlgoName.equals(DOCA_KEY)) {
          algorithm = new Doca();
        } else {
          throw new IllegalArgumentException("Unknown anonymization algorithm passed");
        }
        Serializer<JsonNode> serializer = new Serializer<>();
        Deserializer deserializer = new Deserializer();
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(serializer, deserializer);
        try (InputStream inputStream = Files.newInputStream(Paths.get(userDirectory + "/src/main/resources/kafka.properties"))) {
          props.load(inputStream);
          props.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, "1000"); // Needed to prevent timeouts during broker startup.
          props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
          props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, jsonSerde.getClass());

          // Create anonymization stream and use it with Kafka
          StreamsBuilder streamsBuilder = new StreamsBuilder();
          KStream<String, JsonNode> src = streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), jsonSerde));
          Produced<String, JsonNode> produced = Produced.with(Serdes.String(), jsonSerde);

          DataRepository dataRepository = new DataRepository();
          String[] fields = getFieldsToAnonymize();
          src.mapValues(value -> {
              try {
                  return processing(value, dataRepository, fields, addUnanonymizedHistory);
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
