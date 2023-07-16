package benchmark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class PerformanceBenchmark {
    private static KafkaConsumer<String, String> consumer;
    private static Thread anonymizationThread;
    private void getAnonymizationTime() {

    }
    public static void main(String[] args) throws IOException, ParseException {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty(
            "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(
            "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("output-test"));

        anonymizationThread = new Thread();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                long consumerTimestamp = System.currentTimeMillis();

                MetricsCollector.setConsumerTimestamps(record.key(), consumerTimestamp); // TODO: put the id into the record key

                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}
