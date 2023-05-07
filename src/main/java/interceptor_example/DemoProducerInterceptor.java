package interceptor_example;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class DemoProducerInterceptor implements ProducerInterceptor<String,String>{
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        ProducerRecord<String, String> mutatedRecord =
                new ProducerRecord<>(producerRecord.topic(), producerRecord.key(), producerRecord.value().replaceAll("e", "3").replaceAll("o", "0"));
        return mutatedRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
