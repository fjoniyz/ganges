package myapps;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import serdes.AnonymizedMessage;
import serdes.Deserializer;
import serdes.chargingstation.ChargingStationMessage;
import serdes.Serializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.*;
import serdes.emobility.EMobilityStationMessage;
import serdes.emobility.EMobilityStationSerde;

public class Pipe {

    public static AnonymizedMessage copyMessage(AnonymizedMessage message) {
      if (message.getClass().equals(EMobilityStationMessage.class)) {
        EMobilityStationMessage emobilityMsg = (EMobilityStationMessage) message;
        return new EMobilityStationMessage(new String(emobilityMsg.getId()), new String(emobilityMsg.getTimestamp()), new String(emobilityMsg.getTimeseriesId()), emobilityMsg.getEvUsage());
      }
      else {
        return null; // TODO: add other message types
      }
    }


    public static AnonymizedMessage processing(AnonymizedMessage message, DataRepository dataRepository, String[] fields) throws IOException {

      double[] valuesList = message.getValuesListFromKeys(fields);
      StringBuilder valueToSaveInRedis = new StringBuilder();
      for (double d: valuesList
      ) {
          valueToSaveInRedis.append(d).append(",");
      }
      // Retrieve non-anonymized data from cache
      dataRepository.saveValue(valueToSaveInRedis.toString());
      List<String> allSavedValues = dataRepository.getValues();

      // Parse cached strings into double arrays
      double[][] input = new double[allSavedValues.size()][];

      for (int i = 0; i < allSavedValues.size(); i++) {
          String[] values = allSavedValues.get(i).split(",");
          double[] toDouble =
                  Arrays.stream(values).mapToDouble(Double::parseDouble).toArray();

          input[i] = toDouble;
      }

      // Anonymization
      Doca docaInstance = new Doca();
      double[][] output = docaInstance.anonymize(input);
      String result = Arrays.deepToString(output);
      return result;
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
          DataRepository dataRepository = new DataRepository();
          Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());
          String[] fields = getFieldsToAnonymize();
          src.mapValues(value -> {
              try {
                  return processing(value, dataRepository, fields);
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
