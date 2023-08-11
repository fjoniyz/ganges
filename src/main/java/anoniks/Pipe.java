package anoniks;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ganges.lib.AnonymizationAlgorithm;
import com.ganges.lib.AnonymizationItem;
import com.ganges.lib.DataRepository;
import com.ganges.lib.castleguard.CastleGuard;
import com.ganges.lib.doca.Doca;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import serdes.Deserializer;
import serdes.Serializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Pipe {

    private final static String CASTLEGUARD_KEY = "castleguard";
    private final static String DOCA_KEY = "doca";
    private static AnonymizationAlgorithm algorithm;

    /**
     * This method extracts values from a JSON message based on keys specified in the configuration file.
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

    //TODO: Is this the correct description?
    /**
     * This method extracts values from a JSON message for the specified keys that are not marked for anonymization.
     * Each key represents a header, and the associated non-anonymized value is retrieved from the JSON message.
     * The method iterates through the JSON fields and includes only those that are not present in the list of configured headers.
     *
     * @param jsonNode The root node of the incoming JSON message.
     * @param fields   A list of keys representing the headers to be excluded from anonymization.
     * @return A mapping of header names to their corresponding non-anonymized values from the JSON message.
     */
    private static HashMap<String, String> getNonAnonymizedValuesByKeys(JsonNode jsonNode, List<String> fields) {
        HashMap<String, String> values = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> jsonEntry = it.next();
            if (!fields.contains(jsonEntry.getKey())) {
                values.put(jsonEntry.getKey(), jsonEntry.getValue().textValue());
            }
        }
        return values;
    }

    /**
     * Processes a JSON message for anonymization and generates an output JSON message with anonymized data.
     *
     * @param message                The input JSON message to be processed and anonymized.
     * @param dataRepository         The redis repository for retrieving context information needed for anonymization.
     * @param anonFields             An array of field names representing the data fields to be anonymized.
     * @param addUnanonymizedHistory A flag indicating whether to add unanonymized data history to the context values.
     * @return An output JSON message containing anonymized data.
     * @throws IOException If an I/O error occurs during JSON conversion or data repository access.
     */
    private static JsonNode processing(JsonNode message, DataRepository dataRepository, String[] anonFields,
                                       boolean addUnanonymizedHistory) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> messageMap = mapper.convertValue(message, new TypeReference<>() {
        });

        // Get values of current message
        String id = message.get("id").textValue();
        List<String> anonFieldsList = List.of(anonFields);
        HashMap<String, Double> valuesMap = getValuesListByKeys(message, anonFieldsList);
        HashMap<String, String> nonAnonymizedValuesMap = getNonAnonymizedValuesByKeys(message,
                anonFieldsList);
        System.out.println(message);

        // Get all entries needed for anonymization
        List<AnonymizationItem> contextValues = new ArrayList<>();
        if (addUnanonymizedHistory) {
            dataRepository.open();

            // Retrieve non-anonymized data from cache
            dataRepository.saveValues(messageMap);

            // Here we assume that for one message type the values are stored consistently (all
            // fields are present in cache)
            contextValues = dataRepository.getValuesByKeys(anonFieldsList);
            dataRepository.close();
        } else {
            contextValues.add(new AnonymizationItem(id, valuesMap, nonAnonymizedValuesMap));
        }

        // Anonymization
        List<AnonymizationItem> output = algorithm.anonymize(contextValues);

        ArrayNode outputMessage = getJsonFromItems(output);
        System.out.println("OUTPUT " + outputMessage);
        return outputMessage;
    }

    /**
     * Converts a list of AnonymizationItem objects into a JSON array containing anonymized data.
     *
     * @param outputItems A list of AnonymizationItem objects representing processed records with anonymized data.
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
     * Retrieves an array of fields to be anonymized based on configuration file.
     *
     * @return An array of field names to be anonymized.
     * @throws IOException If an I/O error occurs while reading the configuration file.
     */
    public static String[] getFieldsToAnonymize() throws IOException {
        // TODO: generalize for CastleGuard (use different config) -- Maybe use pipe config to store fields to anonymize instead of doca/castleguard.properties
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
