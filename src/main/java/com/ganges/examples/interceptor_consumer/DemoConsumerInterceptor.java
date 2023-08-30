package com.ganges.examples.interceptor_consumer;

import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

// import static jdk.internal.org.jline.reader.impl.LineReaderImpl.CompletionType.List;
public class DemoConsumerInterceptor implements ConsumerInterceptor<String, String> {

  @Override
  public ConsumerRecords<String, String> onConsume(
      ConsumerRecords<String, String> consumerRecords) {
    Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new LinkedHashMap<>();
    TopicPartition topicpartition = null;
    // Source for iter:
    // https://stackoverflow.com/questions/1839668/what-is-the-best-way-to-combine-two-lists-into-a-map-java
    List<ConsumerRecord<String, String>> mutatedRecords = new ArrayList<>();
    Iterator<ConsumerRecord<String, String>> iter = consumerRecords.iterator();
    while (iter.hasNext()) {
      ConsumerRecord<String, String> element = iter.next();
      topicpartition = new TopicPartition(element.topic(), element.partition());
      ConsumerRecord<String, String> mutatedRecord =
          new ConsumerRecord<>(
              element.topic(),
              element.partition(),
              element.offset(),
              element.key(),
              element.value().replaceAll("e", "3").replaceAll("4", "x"));
      mutatedRecords.add(mutatedRecord);
    }
    records.put(topicpartition, mutatedRecords);

    return new ConsumerRecords<>(records);
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {}

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> map) {}
}
