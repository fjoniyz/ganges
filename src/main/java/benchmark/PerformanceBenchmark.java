package benchmark;

import com.ganges.examples.interceptor_consumer.DemoConsumerInterceptor;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class PerformanceBenchmark {
    private static KafkaConsumer<String, String> consumer;
    private static Thread anonymizationThread;
    private void getAnonymizationTime() {

    }
    public static void main(String[] args) {
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
        consumer.subscribe(Arrays.asList("energy-usage"));

        anonymizationThread = new Thread();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf(
                    "offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
