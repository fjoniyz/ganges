package benchmark;

import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class PerformanceBenchmark {

  private void getAnonymizationTime() {

  }

  public static void main(String[] args) throws IOException, ParseException, ExecutionException,
      InterruptedException {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("group.id", "test");
    props.setProperty("enable.auto.commit", "true");
    props.setProperty("auto.commit.interval.ms", "1000");
    props.setProperty(
        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.setProperty(
        "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList("output-test"));

    LocalMetricsCollector metricsCollector = LocalMetricsCollector.getInstance();
    //metricsCollector.setFileName("consumer_results.csv");
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records) {
        long consumerTimestamp = System.currentTimeMillis();

        //LocalMetricsCollector.setConsumerTimestamps(record.key(), consumerTimestamp); // TODO:
        // put the
        // id into the
        // record key
        //metricsCollector.saveMetricsToCSV();

        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
      }
    }
  }
}
