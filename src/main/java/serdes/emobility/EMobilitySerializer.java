package serdes.emobility;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class EMobilitySerializer<T> implements Serializer<T> {
  private final ObjectMapper om = new ObjectMapper();
  @Override
  public byte[] serialize(String s, T o) {
    byte[] retval;
    try {
      System.out.println("Class: " + o.getClass());
      retval = om.writeValueAsString(o).getBytes();
    } catch (JsonProcessingException e) {
      throw new SerializationException(e);
    }
    return retval;
  }
}
