package myapps;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import customSerdes.ChargingStationDeserializer;
import customSerdes.ChargingStationMessage;
import customSerdes.ChargingStationSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.*;

public class Pipe {
    public static String processing(ChargingStationMessage value, DataRepository dataRepository, String[] fields) {
            double[] valuesList = createValuesList(fields, value);
            System.out.println("Parsed values: " + value);
            // Retrieve non-anonymized data from cache
            dataRepository.saveValue(Arrays.toString(valuesList));
            List<String> allSavedValues = dataRepository.getValues();

            // Parse cached strings into double arrays
            double[][] input = new double[allSavedValues.size()][];
            for (int i = 0; i < allSavedValues.size(); i++) {
                String[] values = allSavedValues.get(i).split(",");
                double[] toDouble =
                        Arrays.stream(values).mapToDouble(Double::parseDouble).toArray();
                input[i] = toDouble;
            }

            // Log retrieved arrays
            for (double[] element : input) {
                System.out.print("Saved Value: ");
                for (double d : element) {
                    System.out.print(d + " ");
                }
                System.out.println();
            }

            // Anonymization
            double[][] output = Doca.doca(input, 99999, 1000, 60, false);
            String result = Arrays.deepToString(output);
            System.out.println("Result: " + result);
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
                    System.out.println("urb level: " + chargingStationMessage.getUrbanisation_level());
                    values.add((double) chargingStationMessage.getUrbanisation_level());
                    break;
                case "number_loading_stations":
                    System.out.println("number load stat: " + chargingStationMessage.getNumber_loading_stations());
                    values.add((double) chargingStationMessage.getNumber_loading_stations());
                    break;
                case "number_parking_spaces":
                    System.out.println("parking spaces" + chargingStationMessage.getNumber_parking_spaces());
                    values.add((double) chargingStationMessage.getNumber_parking_spaces());
                    break;
                case "start_time_loading":
                    System.out.println("loading time start" + chargingStationMessage.getStart_time_loading());
                    values.add((double) chargingStationMessage.getStart_time_loading());
                    break;
                case "end_time_loading":
                    System.out.println("load time end" + chargingStationMessage.getEnd_time_loading());
                    values.add((double) chargingStationMessage.getEnd_time_loading());
                    break;
                case "loading_time":
                    System.out.println("loading time" + chargingStationMessage.getLoading_time());
                    values.add((double) chargingStationMessage.getLoading_time());
                    break;
                case "kwh":
                    System.out.println("kwh: " + chargingStationMessage.getKwh());
                    values.add((double) chargingStationMessage.getKwh());
                    break;
                case "loading_potential":
                    System.out.println("load potential " + chargingStationMessage.getLoading_potential());
                    values.add((double) chargingStationMessage.getLoading_potential());
                    break;
                default:
                    System.out.println("Invalid field in config file: " + field);
            }
        }
        return values.stream().mapToDouble(d -> d).toArray();
    }

    public static void main(final String[] args) {
        String userDirectory = System.getProperty("user.dir");
        try (InputStream inputStream = Files.newInputStream(Paths.get(userDirectory + "/src/main/resources/config.properties"))) {
            Properties props = new Properties();
            String inputTopic = "input2";
            String outputTopic = "output-test";

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
            src.mapValues(value -> processing(value, dataRepository, fields)).to(outputTopic, produced);
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
