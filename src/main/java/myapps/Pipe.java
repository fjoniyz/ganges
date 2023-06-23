package myapps;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import customSerdes.ChargingStationDeserializer;
import customSerdes.ChargingStationMessage;
import customSerdes.ChargingStationSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import com.lambdaworks.redis.*;
import org.apache.kafka.streams.kstream.Produced;

import java.util.*;

public class Pipe {

    public static List<String> getKeys(){
        List<String> result = new ArrayList<>();
        String userDirectory = System.getProperty("user.dir");
        System.out.printf("Dir: " + userDirectory);
        try {
            Runtime r = Runtime.getRuntime();

            Process p = r.exec(userDirectory + "/getKeys.sh");

            BufferedReader in =
                    new BufferedReader(new InputStreamReader(p.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                System.out.println(inputLine);
                result.add(inputLine);
            }
            in.close();

        } catch (IOException e) {
            System.out.println(e);
        }
        return result;
    }

    public static List<String> getValues(List<String> keys, RedisConnection<String, String> connection){
        List<String> values = new ArrayList<>();
        for(String key: keys){
            values.add(connection.get(key));
        }
        return values;
    }

    public static String[] getFieldsToAnonymize() throws IOException {
        String userDirectory = System.getProperty("user.dir");
        try(InputStream inputStream = Files.newInputStream(Paths.get(userDirectory + "/src/main/resources/doca.properties"))) {
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

    public static double[] createValuesList(String[] fields, ChargingStationMessage chargingStationMessage){
        List<Double> values = new ArrayList<>();
        for (String field: fields) {
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
            RedisClient redisClient = new RedisClient(RedisURI.create("redis://localhost/"));
            RedisConnection<String, String> connection = redisClient.connect();

            String inputTopic = "input-json2";
            String outputTopic = "output-test";

            ChargingStationSerializer<ChargingStationMessage> chargingStationSerializer = new ChargingStationSerializer<>();
            ChargingStationDeserializer<ChargingStationMessage> chargingStationDeserializer = new ChargingStationDeserializer<>(ChargingStationMessage.class);
            Serde<ChargingStationMessage> chargingStationMessageSerde = Serdes.serdeFrom(chargingStationSerializer, chargingStationDeserializer);

            props.load(inputStream);
            props.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, "1000"); // Needed to prevent timeouts during broker startup.
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, chargingStationMessageSerde.getClass());

            Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

            final StreamsBuilder streamsBuilder = new StreamsBuilder();
            KStream<String, ChargingStationMessage> src = streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), chargingStationMessageSerde));
            String[] fields = getFieldsToAnonymize();
            for(String s : fields){
                System.out.println(s);
            }
            src.mapValues(value -> {
                double[] valuesList = createValuesList(fields, value);
                String parsedValue = Arrays.toString(valuesList).substring(1, Arrays.toString(valuesList).length() - 1);
                System.out.println("Parsed values: " + parsedValue);
                String key = Integer.toString(ThreadLocalRandom.current().nextInt(0, 1000 + 1));
                connection.set(key, parsedValue);
                List<String> keys = getKeys();
                for (String k: keys
                ) {
                    System.out.println("key in: " + k);
                }
                List<String> valuesFromRedis = getValues(keys, connection);
                for(String v: valuesFromRedis){
                    System.out.println("Value from inside: " + v);
                }
                double[][] input = new double[valuesFromRedis.size()][];
                for (int i = 0; i < valuesFromRedis.size(); i++) {
                    String[] values = valuesFromRedis.get(i).split(",");
                    double[] toDouble = Arrays.stream(values)
                            .mapToDouble(Double::parseDouble)
                            .toArray();
                    input[i] = toDouble;
                }
                for(double[] element: input){
                    for(double d : element){
                        System.out.println("D: " + d);
                    }
                    System.out.println();
                }

                double[][] output = Doca.doca(input, 99999, 1000, 60, false);
                System.out.println("Output: " + output);
                String result = Arrays.deepToString(output);
                System.out.println("Result: " + result);
                return result;
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
