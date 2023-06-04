package myapps;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;
//ATM not being used but may be used later to serde the value of each record
//For that we need to create a way to test/make sure that the value of each record that we send is a double[]
//If we send it from the terminal it is going to be sent as a string that's why it cannot be used as a class for now
public class DoubleArraySerde implements Serde<double[]> {
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // No additional configuration needed
  }

  @Override
  public void close() {
    // No resources to release
  }

  @Override
  public Serializer<double[]> serializer() {
    return (topic, data) -> {
      if (data == null) {
        return null;
      }

      ByteBuffer buffer = ByteBuffer.allocate(data.length * Double.BYTES);
      for (double value : data) {
        buffer.putDouble(value);
      }

      return buffer.array();
    };
  }

  @Override
  public Deserializer<double[]> deserializer() {
    return (topic, data) -> {
      if (data == null) {
        return null;
      }

      int length = data.length / Double.BYTES;
      double[] result = new double[length];
      ByteBuffer buffer = ByteBuffer.wrap(data);

      for (int i = 0; i < length; i++) {
        result[i] = buffer.getDouble();
      }

      return result;
    };
  }
}
