package serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationException;

public class Serializer<T> implements org.apache.kafka.common.serialization.Serializer<T> {
    private final ObjectMapper om = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, T o) {
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
