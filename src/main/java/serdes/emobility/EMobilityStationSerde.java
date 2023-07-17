package serdes.emobility;

import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import serdes.Deserializer;
import serdes.Serializer;

public class EMobilityStationSerde implements Serde<EMobilityStationMessage> {
    final private Serializer<EMobilityStationMessage>
        serializer;
    final private Deserializer<EMobilityStationMessage>
        deserializer;

    public EMobilityStationSerde(Serializer<EMobilityStationMessage> serializer, Deserializer<EMobilityStationMessage> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public Serializer<EMobilityStationMessage> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<EMobilityStationMessage> deserializer() {
        return deserializer;
    }
}
