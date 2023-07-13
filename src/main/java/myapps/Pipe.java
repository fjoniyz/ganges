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
    public static String processing(ChargingStationMessage value, DataRepository dataRepository, String[] fields) throws IOException {
        double[] valuesList = createValuesList(fields, value);
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
        double[][] output = docaInstance.addData(input);
        if (output.length == 0 || output[0].length == 0) {
            return null;
        }
        return Arrays.deepToString(output);
    }

    public static String processingWithDelta(ChargingStationMessage value){
        //TODO: Implement Doca with delta(a.k.a suppression)
        return "";
    }

    public static PropertiesToReturn getFieldsToAnonymize() throws IOException {
        String userDirectory = System.getProperty("user.dir");
        try (InputStream inputStream = Files.newInputStream(Paths.get(userDirectory + "/src/main/resources/doca.properties"))) {
            Properties properties = new Properties();
            properties.load(inputStream);
            String docaFieldsString = properties.getProperty("doca_fields");
            boolean isDeltaWanted = Boolean.getBoolean(properties.getProperty("delta"));
            String[] docaFields = docaFieldsString.split(",");
            for (String field : docaFields) {
                System.out.println(field);
            }
            return new PropertiesToReturn(isDeltaWanted, docaFields);
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
        try (InputStream inputStream = Files.newInputStream(Paths.get(userDirectory + "/src/main/resources/config.properties"))) {
            Properties props = new Properties();
            String inputTopic = "input-test3";
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
            PropertiesToReturn propertiesToReturn = getFieldsToAnonymize();
            String[] fields = propertiesToReturn.docaFields;

            /*
            if(!propertiesToReturn.isDeltaWanted){
                src.mapValues(value -> {
                    try {
                        return processing(value, dataRepository, fields);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).to(outputTopic, produced);
            } else {
                src.mapValues(Pipe::processingWithDelta).to(outputTopic, produced);
            }*/
            src.mapValues(value -> {
                try {
                    String res = processing(value, dataRepository, fields);
                    if (res != null) {
                        return res;
                    } else
                        return null;
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
