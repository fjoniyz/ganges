package myapps;

<<<<<<< HEAD
import customSerdes.ChargingStationSerde;
import java.io.*;
import java.nio.charset.StandardCharsets;
=======
import java.io.IOException;
import java.io.InputStream;
>>>>>>> main
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import java.util.stream.Collectors;
import serdes.AnonymizedMessage;
import serdes.Deserializer;
import serdes.chargingstation.ChargingStationMessage;
import serdes.Serializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.*;
import serdes.emobility.EMobilityStationMessage;

public class Pipe {
<<<<<<< HEAD
    public static String processing(ChargingStationMessage value, String[] fields, DataRepository dataRepository) throws IOException {
        double[] valuesList = createValuesList(fields, value);
        StringBuilder valueToSaveInRedis = new StringBuilder();
        for (double d: valuesList
        ) {
            valueToSaveInRedis.append(d).append(",");
        }
        // Retrieve non-anonymized data from cache
        dataRepository.saveValue(valueToSaveInRedis.toString());
        List<String> allSavedValues = dataRepository.getValues();

        double[][] input = new double[allSavedValues.size()][];
        input[0] = valuesList;
=======

    public static AnonymizedMessage createMessage(Class<?> messageClass, Object... values) {
      AnonymizedMessage message = null;
      if (messageClass.equals(EMobilityStationMessage.class)) {
//        message = new EMobilityStationMessage(values[0], values);
        return message;
      }
      else {
        return null; // TODO: add other message types
      }
    }
>>>>>>> main


    public static <T extends AnonymizedMessage> String processing(T message, DataRepository dataRepository, String[] fields) throws IOException {
      String id = message.getId();
      Double[] valuesList = message.getValuesListByKeys(fields);
      dataRepository.open();

<<<<<<< HEAD
        // Anonymization

        Doca docaInstance = new Doca();
        double[][] output = docaInstance.anonymize(input);
        double[] lastItem = output[output.length-1];
        float kwh = (float)lastItem[0];
        float load_potential = (float)lastItem[1];
        value.setKwh(kwh);
        value.setLoadingPotential(load_potential);
        Serializer<ChargingStationMessage> serializer = new ChargingStationSerializer<>();
        byte[] json = serializer.serialize("output", value);
        return new String(json, StandardCharsets.UTF_8);
=======
      // Retrieve non-anonymized data from cache
      HashMap<String, Double> keyValueMap = new HashMap<>();
      for (int i = 0; i < valuesList.length; i++) {
        keyValueMap.put(fields[i], valuesList[i]);
      }
      dataRepository.saveValues(id, keyValueMap);

      // Here we assume that for one message type the values are stored consistently
      List<Map<String, Double>> allSavedValues = dataRepository.getValuesByKeys(fields);
      dataRepository.close();

      // Anonymization
      Doca doca = new Doca();
      List<Map<String, Double>> output = doca.anonymize(allSavedValues);

      String outputString =
          output.stream().map(dataRow -> dataRow.values().toString()).collect(Collectors.joining(
              ","));
      System.out.println("result: " + outputString);
      return outputString;
>>>>>>> main
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
<<<<<<< HEAD
        try (InputStream inputStream = Files.newInputStream(Paths.get(userDirectory + "/src/main/resources/config.properties"))) {
            Properties props = new Properties();
            String inputTopic = "input-topicc";
            String outputTopic = "output-topic";

            ChargingStationSerializer<ChargingStationMessage> chargingStationSerializer = new ChargingStationSerializer<>();
            ChargingStationDeserializer<ChargingStationMessage> chargingStationDeserializer = new ChargingStationDeserializer<>(ChargingStationMessage.class);
            Serde<ChargingStationMessage> chargingStationMessageSerde = Serdes.serdeFrom(chargingStationSerializer, chargingStationDeserializer);

            props.load(inputStream);
            props.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, "1000"); // Needed to prevent timeouts during broker startup.
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, chargingStationMessageSerde.getClass());

            // Create anonymization stream and use it with Kafka
            StreamsBuilder streamsBuilder = new StreamsBuilder();
            KStream<String, ChargingStationMessage> src = streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), chargingStationMessageSerde));
            DataRepository dataRepository = new DataRepository();
            Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());
            String[] fields = getFieldsToAnonymize();
            src.mapValues(value -> {
                try {
                    return processing(value, fields, dataRepository);
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
=======
        Properties props = new Properties();

        try (InputStream inputStream = Files.newInputStream(Paths.get(userDirectory + "/src/main/resources/pipe.properties"))) {
          props.load(inputStream);
>>>>>>> main
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
          Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

          DataRepository dataRepository = new DataRepository();
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
