package myapps;

import com.ganges.lib.castleguard.CastleGuard;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import serdes.AnonymizedMessage;
import serdes.Deserializer;
import serdes.Serializer;
import serdes.chargingstation.ChargingStationMessage;
import serdes.emobility.EMobilityStationMessage;

public class Pipe {

    private final static String CASTLEGUARD_KEY = "castleguard";
    private final static String DOCA_KEY = "doca";
    private static AnonymizationAlgorithm algorithm;

  public static AnonymizedMessage createMessage(Class<?> messageClass, Object... values) {
      AnonymizedMessage message = null;
      if (messageClass.equals(EMobilityStationMessage.class)) {
//        message = new EMobilityStationMessage(values[0], values[1], values[2], values[3]);
        return message;
      }
      else {
        return null; // TODO: add other message types
      }
    }


    public static <T extends AnonymizedMessage> String processing(T message,
                                                                  DataRepository dataRepository,
                                                                  String[] fields,
                                                                  boolean addUnanonymizedHistory) throws IOException {
      String id = message.getId();
      Double[] valuesList = message.getValuesListByKeys(fields);
      List<AnonymizationItem> contextValues = new ArrayList<>();

      // Current message
      HashMap<String, Double> keyValueMap = new HashMap<>();
      for (int i = 0; i < valuesList.length; i++) {
        keyValueMap.put(fields[i], valuesList[i]);
      }

      if (addUnanonymizedHistory) {
        dataRepository.open();

        // Retrieve non-anonymized data from cache
        dataRepository.saveValues(id, keyValueMap);

        // Here we assume that for one message type the values are stored consistently
        contextValues = dataRepository.getValuesByKeys(fields);
        dataRepository.close();
      } else {
        contextValues.add(new AnonymizationItem(id, keyValueMap));
      }
      // Anonymization
      List<AnonymizationItem> output = algorithm.anonymize(contextValues);

      if (output.isEmpty()) {
        //TODO: Check what to return when no output is present
        return "";
      }


      String outputString =
          output.stream().map(dataRow -> dataRow.getValues().toString()).collect(Collectors.joining(
              ","));
      System.out.println("result: " + outputString);
      return outputString;
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

    public static double[] createValuesList(String[] fields, ChargingStationMessage chargingStationMessage) {
        List<Double> values = new ArrayList<>();
        for (String field : fields) {
            switch (field) {
                case "urbanisation_level":
                    System.out.println("urb level: " + chargingStationMessage.getUrbanisationLevel());
                    values.add((double) chargingStationMessage.getUrbanisationLevel());
                    break;
                case "number_loading_stations":
                    System.out.println("number load stat: " + chargingStationMessage.getNumberLoadingStations());
                    values.add((double) chargingStationMessage.getNumberLoadingStations());
                    break;
                case "number_parking_spaces":
                    System.out.println("parking spaces" + chargingStationMessage.getNumberParkingSpaces());
                    values.add((double) chargingStationMessage.getNumberParkingSpaces());
                    break;
                case "start_time_loading":
                    System.out.println("loading time start" + chargingStationMessage.getStartTimeLoading());
                    values.add((double) chargingStationMessage.getStartTimeLoading());
                    break;
                case "end_time_loading":
                    System.out.println("load time end" + chargingStationMessage.getEndTimeLoading());
                    values.add((double) chargingStationMessage.getEndTimeLoading());
                    break;
                case "loading_time":
                    System.out.println("loading time" + chargingStationMessage.getLoadingTime());
                    values.add((double) chargingStationMessage.getLoadingTime());
                    break;
                case "kwh":
                    System.out.println("kwh: " + chargingStationMessage.getKwh());
                    values.add((double) chargingStationMessage.getKwh());
                    break;
                case "loading_potential":
                    System.out.println("load potential " + chargingStationMessage.getLoadingPotential());
                    values.add((double) chargingStationMessage.getLoadingPotential());
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

        Serializer<EMobilityStationMessage> serializer = new Serializer<>();
        Deserializer<EMobilityStationMessage>
            emobilityDeserializer = new Deserializer<>(EMobilityStationMessage.class);
        Serde<EMobilityStationMessage> emobilitySerde = Serdes.serdeFrom(serializer, emobilityDeserializer);

        try (InputStream inputStream = Files.newInputStream(Paths.get(userDirectory + "/src/main/resources/kafka.properties"))) {
          props.load(inputStream);
          props.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, "1000"); // Needed to prevent timeouts during broker startup.
          props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
          props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, emobilitySerde.getClass());

          // Create anonymization stream and use it with Kafka
          StreamsBuilder streamsBuilder = new StreamsBuilder();
          KStream<String, EMobilityStationMessage> src = streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), emobilitySerde));
          Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

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
