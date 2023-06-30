package myapps;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import com.lambdaworks.redis.*;

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

  public static void main(final String[] args) {
    String userDirectory = System.getProperty("user.dir");
    try (InputStream inputStream = Files.newInputStream(Paths.get(userDirectory + "/src/main/resources/config.properties"))) {
      Properties props = new Properties();
      RedisClient redisClient = new RedisClient(RedisURI.create("redis://localhost/"));
      RedisConnection<String, String> connection = redisClient.connect();


      final List<double[]> saved = new ArrayList<>();
      String inputTopic = "input";
      String outputTopic = "output-test";

      props.load(inputStream);
      props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      props.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, "1000"); // Needed to prevent timeouts during broker startup.

      final StreamsBuilder streamsBuilder = new StreamsBuilder();
      KStream<String, String> src = streamsBuilder.stream(inputTopic);
      src.mapValues(value -> {
        String parsedValue = value.substring(1, value.length() - 1);
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
      }).to(outputTopic);


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
