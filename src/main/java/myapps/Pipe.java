package myapps;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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

public class Pipe {

    public static AnonymizedMessage createMessage(Class<?> messageClass, Object... values) {

        AnonymizedMessage message = null;
        if (messageClass.equals(EMobilityStationMessage.class)) {
            // message = new EMobilityStationMessage(values[0], values);
            return message;
        } else {
            return null; // TODO: add other message types
        }
    }


    public static <T extends AnonymizedMessage> String processing(T message,
                                                                  DataRepository dataRepository,
                                                                  String[] fields) throws IOException {
        String id = message.getId();
        double[] valuesList = message.getValuesListFromKeys(fields);
        dataRepository.open();

        // Retrieve non-anonymized data from cache
        HashMap<String, Double> keyValueMap = new HashMap<>();
        for (int i = 0; i < valuesList.length; i++) {
            keyValueMap.put(fields[i], valuesList[i]);
        }
        dataRepository.saveValues(keyValueMap);

        // Here we assume that for one message type the values are stored consistently
        List<Map<String, Double>> allSavedValues = dataRepository.getValuesByKeys(fields);
        dataRepository.close();

        // Anonymization
        Doca doca = new Doca();
        List<Map<String, Double>> output = doca.anonymize(allSavedValues);
        Map<String, Double> lastItem = output.get(output.size() - 1);
        ((ChargingStationMessage) message).setKwh(lastItem.get("kwh").floatValue());
        ((ChargingStationMessage) message).setLoadingPotential(lastItem.get("loading_potential").floatValue());
//        ((ChargingStationMessage) message).setStartTimeLoading(lastItem.get("start_time_loading").longValue());
        Serializer<ChargingStationMessage> serializer = new Serializer<>();
        byte[] json = serializer.serialize("output", (ChargingStationMessage) message);
        return new String(json, StandardCharsets.UTF_8);
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

        Serializer<ChargingStationMessage> serializer = new Serializer<>();
        Deserializer<ChargingStationMessage>
                emobilityDeserializer = new Deserializer<>(ChargingStationMessage.class);
        Serde<ChargingStationMessage> emobilitySerde = Serdes.serdeFrom(serializer, emobilityDeserializer);

        try (InputStream inputStream = Files.newInputStream(Paths.get(userDirectory + "/src/main/resources/kafka.properties"))) {
            props.load(inputStream);
            props.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, "1000"); // Needed to prevent timeouts during broker startup.
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, emobilitySerde.getClass());

            // Create anonymization stream and use it with Kafka
            StreamsBuilder streamsBuilder = new StreamsBuilder();
            KStream<String, ChargingStationMessage> src = streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), emobilitySerde));
            Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

            DataRepository dataRepository = new DataRepository();
            String[] fields = getFieldsToAnonymize();
            src.mapValues(
                            value -> {
                                try {
                                    return processing(value, dataRepository, fields);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            })
                    .map(
                            (key, value) -> {
                                System.out.println("result: " + key + " ; " + value);
                                return new KeyValue<>(key, value);
                            })
                    .to(outputTopic, produced);
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
