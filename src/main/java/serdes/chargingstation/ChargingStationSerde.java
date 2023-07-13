package serdes.chargingstation;

import org.apache.kafka.common.serialization.Serde;

import java.util.Map;
import serdes.Serializer;
import serdes.Deserializer;

public class ChargingStationSerde implements Serde<ChargingStationMessage> {
    final private Serializer<ChargingStationMessage> serializer;
    final private Deserializer<ChargingStationMessage> deserializer;

    public ChargingStationSerde(Serializer<ChargingStationMessage> serializer, Deserializer<ChargingStationMessage> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public org.apache.kafka.common.serialization.Serializer<ChargingStationMessage> serializer() {
        return serializer;
    }

    @Override
    public org.apache.kafka.common.serialization.Deserializer<ChargingStationMessage> deserializer() {
        return deserializer;
    }
}
