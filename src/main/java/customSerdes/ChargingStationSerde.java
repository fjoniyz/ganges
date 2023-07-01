package customSerdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ChargingStationSerde implements Serde<ChargingStationMessage> {
    final private ChargingStationSerializer<ChargingStationMessage> serializer;
    final private ChargingStationDeserializer<ChargingStationMessage> deserializer;

    public ChargingStationSerde(ChargingStationSerializer<ChargingStationMessage> serializer, ChargingStationDeserializer<ChargingStationMessage> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public Serializer<ChargingStationMessage> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<ChargingStationMessage> deserializer() {
        return deserializer;
    }
}
