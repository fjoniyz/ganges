package myapps;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

class Pipe {

    public static void main(final String[] args) {
	Properties props = new Properties();
	props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
	props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

	final StreamsBuilder builder = new StreamsBuilder();
	KStream<String, String> src = builder.stream("streams-plaintext-input");
	src.mapValues(value -> value + "X").to("streams-pipe-output");

	final Topology topology = builder.build();
	final KafkaStreams streams = new KafkaStreams(topology, props);
	final CountDownLatch latch = new CountDownLatch(1);

	try {
	    streams.start();
	    latch.await();
	} catch (Throwable e) {
	    System.exit(1);
	}
	System.exit(0);
    }
}
