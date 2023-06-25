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

import javax.xml.crypto.Data;
import java.util.*;

public class Pipe {

    public static StreamsBuilder getAnonymizationStream(String inputTopic, String outputTopic) {
      DataRepository dataRepository = new DataRepository();
      final StreamsBuilder streamsBuilder = new StreamsBuilder();
      KStream<String, String> src = streamsBuilder.stream(inputTopic);
      src.mapValues(value ->  value.substring(1, value.length() - 1))
          .mapValues( parsedValue -> {
            System.out.println("Parsed values: " + parsedValue);
            // Retrieve non-anonymized data from cache
            dataRepository.saveValue(parsedValue);
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
            return output;
          })
          .mapValues(anonymizedData -> {
            String result = Arrays.deepToString(anonymizedData);
            System.out.println("Result: " + result);
            return result;
          })
          .to(outputTopic);
      return streamsBuilder;
    }

    public static void main(final String[] args) {
        String userDirectory = System.getProperty("user.dir");
        try (InputStream inputStream = Files.newInputStream(Paths.get(userDirectory + "/src/main/resources/config.properties"))) {
            Properties props = new Properties();

            String inputTopic = "input";
            String outputTopic = "output-test";

            props.load(inputStream);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, "1000"); // Needed to prevent timeouts during broker startup.

            StreamsBuilder streamsBuilder = getAnonymizationStream(inputTopic, outputTopic);
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
