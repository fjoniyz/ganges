package myapps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ganges.lib.castleguard.CastleGuard;
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

    public static Double[] getValuesListByKeys(JsonNode jsonNode, String[] fields){
        List<Double> values = new ArrayList<>();
        for (String field : fields) {
            if (field.equals("Seconds_EnergyConsumption")) {
                values.add(jsonNode.get("Seconds_EnergyConsumption").doubleValue());
            } else {
                System.out.println("Invalid field in config file: " + field);
            }
        }
        return values.toArray(new Double[values.size()]);
    }


    public static JsonNode processing(JsonNode message, DataRepository dataRepository, String[] fields, boolean addUnanonymizedHistory) throws IOException {
      String id = message.get("id").textValue();
      Double[] valuesList = getValuesListByKeys(message, fields);
      List<AnonymizationItem> contextValues = new ArrayList<>();

      // Current message
      HashMap<String, Double> keyValueMap = new HashMap<>();
      for (int i = 0; i < valuesList.length; i++) {
        keyValueMap.put(fields[i], valuesList[i]);
      }

      if (addUnanonymizedHistory) {
        dataRepository.open();

        // Retrieve non-anonymized data from cache
        dataRepository.saveValues(keyValueMap);

        // Here we assume that for one message type the values are stored consistently
        contextValues = dataRepository.getValuesByKeys(fields);
        dataRepository.close();
      } else {
        contextValues.add(new AnonymizationItem(id, keyValueMap));
      }
      // Anonymization
      List<AnonymizationItem> output = algorithm.anonymize(contextValues);
      Map<String, Double> lastItem = output.get(output.size() - 1).getValues();
      if (output.isEmpty()) {
        //TODO: Check what to return when no output is present
      }

      return ((ObjectNode)message).put("Seconds_EnergyConsumption", lastItem.get("Seconds_EnergyConsumption").floatValue());
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

    public static double[] createValuesList(String[] fields, JsonNode message) {
        List<Double> values = new ArrayList<>();
        for (String field : fields) {
            switch (field) {
                case "urbanisation_level":
                    System.out.println("urb level: " + message.get("urbanisation_level"));
                    values.add(message.get("urbanisation_level").doubleValue());
                    break;
                case "number_loading_stations":
                    System.out.println("number load stat: " + message.get("number_loading_stations"));
                    values.add(message.get("number_loading_stations").doubleValue());
                    break;
                case "number_parking_spaces":
                    System.out.println("parking spaces" + message.get("number_parking_spaces").doubleValue());
                    values.add(message.get("number_parking_spaces").doubleValue());
                    break;
                case "start_time_loading":
                    System.out.println("loading time start" + message.get("start_time_loading").doubleValue());
                    values.add(message.get("start_time_loading").doubleValue());
                    break;
                case "end_time_loading":
                    System.out.println("load time end" + message.get("end_time_loading").doubleValue());
                    values.add(message.get("end_time_loading").doubleValue());
                    break;
                case "loading_time":
                    System.out.println("loading time" + message.get("loading_time").doubleValue());
                    values.add(message.get("loading_time").doubleValue());
                    break;
                case "kwh":
                    System.out.println("kwh: " + message.get("kwh").doubleValue());
                    values.add(message.get("kwh").doubleValue());
                    break;
                case "loading_potential":
                    System.out.println("load potential " + message.get("loading_potential").doubleValue());
                    values.add(message.get("loading_potential").doubleValue());
                    break;
                default:
                    System.out.println("Invalid field in config file: " + field);
            }
        }
        return values.stream().mapToDouble(d -> d).toArray();
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
