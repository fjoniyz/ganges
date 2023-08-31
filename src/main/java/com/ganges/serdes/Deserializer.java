package com.ganges.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class Deserializer implements org.apache.kafka.common.serialization.Deserializer<com.fasterxml.jackson.databind.JsonNode> {
    private final ObjectMapper mapper = new ObjectMapper();
    @Override
    public com.fasterxml.jackson.databind.JsonNode deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, com.fasterxml.jackson.databind.JsonNode.class);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }
}
