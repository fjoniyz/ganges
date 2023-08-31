package com.ganges.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Serializer<JsonNode>
    implements org.apache.kafka.common.serialization.Serializer<JsonNode> {
  private ObjectMapper mapper = new ObjectMapper();

  @Override
  public byte[] serialize(String topic, JsonNode data) {

    try {
      return mapper.writeValueAsBytes(data);
    } catch (JsonProcessingException e) {
      return new byte[0];
    }
  }
}
