package serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.commons.lang3.SerializationException;

public class Deserializer<T> implements org.apache.kafka.common.serialization.Deserializer<T> {
    private final ObjectMapper om = new ObjectMapper();
    private Class<T> type;

    public Deserializer(Class<T> type){
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (type == null) {
            type = (Class<T>) configs.get("type");
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        T data = null;
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        try {
            data = om.readValue(bytes, type);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }
}
