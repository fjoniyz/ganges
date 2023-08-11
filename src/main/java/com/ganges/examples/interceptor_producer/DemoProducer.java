package com.ganges.examples.interceptor_producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class DemoProducer {
  static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
  static final String MAIN_TOPIC = "energy-usage";

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, DemoProducerInterceptor.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    for (int i = 0; i < 10; i++) {
      ProducerRecord<String, String> producerRecord =
          new ProducerRecord<>(MAIN_TOPIC, "test_record", "Hello World");

      producer.send(producerRecord);
    }
    producer.flush();
    producer.close();
  }
}
