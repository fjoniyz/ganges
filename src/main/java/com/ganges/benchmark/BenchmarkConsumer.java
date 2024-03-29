package com.ganges.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.ganges.serdes.Deserializer;

public class BenchmarkConsumer {

  public static void main(String[] args) throws IOException, ParseException, ExecutionException,
      InterruptedException {
    String userDirectory = System.getProperty("user.dir");
    Properties consumerProps = new Properties();
    try (InputStream inputStream = Files.newInputStream(
        Paths.get(userDirectory + "/src/main/resources/monitoring/consumer.properties"))) {
      consumerProps.load(inputStream);
    }

    Properties props = new Properties();
    Deserializer deserializer = new Deserializer();

    props.setProperty("bootstrap.servers", consumerProps.getProperty("bootstrap-server"));
    props.setProperty("group.id", "test");
    props.setProperty("enable.auto.commit", "true");
    props.setProperty("auto.commit.interval.ms", "1000");
    props.put("key.deserializer", deserializer.getClass());
    props.put("value.deserializer", deserializer.getClass());
    KafkaConsumer<String, JsonNode> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(List.of(consumerProps.getProperty("topic")));

    String idKey = consumerProps.getProperty("id-key");
    LocalMetricsCollector metricsCollector = LocalMetricsCollector.getInstance();
    while (true) {
      ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, JsonNode> record : records) {
        long consumerTimestamp = System.currentTimeMillis();

        if (!record.value().isEmpty()) {
          if (record.value().isArray()) {
            for (JsonNode node : record.value()) {
              String id = node.get(idKey).textValue();
              metricsCollector.setConsumerTimestamps(id, consumerTimestamp);
            }
            metricsCollector.sendCurrentResultsToRemote();
          } else {
            String id = record.value().get(idKey).textValue();
            metricsCollector.setConsumerTimestamps(id, consumerTimestamp);
            metricsCollector.sendCurrentResultsToRemote();
          }
        }

        System.out.printf("offset = %d, key = %s, value = %s%n",
            record.offset(), record.key(), record.value());
      }
    }
  }
}
