package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

class Pipe {

    public static void main(final String[] args) {
        Properties props = new Properties();
        String inputTopic = EnvTools.getEnvValue(EnvTools.INPUT_TOPIC, "streams-input");
        String outputTopic = EnvTools.getEnvValue(EnvTools.OUTPUT_TOPIC, "streams-output");
        String bootstrapServerConfig = EnvTools.getEnvValue(EnvTools.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerConfig);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, "500" );   // Needed to prevent timeouts during broker startup.

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> src = builder.stream(inputTopic);
        src.mapValues(value -> value + "X")
                .to(outputTopic);

        final Topology topology = builder.build();
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
    }
}
