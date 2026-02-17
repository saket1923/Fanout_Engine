package com.fanout.transformers;

import com.fanout.model.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Converts a {@link Record} into a JSON payload (String) for REST sinks.
 */
public class RestTransformer implements Transformer<String> {

  private final ObjectMapper objectMapper;

  public RestTransformer() {
    this(new ObjectMapper());
  }

  public RestTransformer(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public String transform(Record record) {
    try {
      // Jackson can serialize Java records directly.
      return objectMapper.writeValueAsString(record);
    } catch (JsonProcessingException e) {
      // Placeholder behavior: return a minimal fallback string.
      System.err.println("RestTransformer: failed to serialize record to JSON: " + e.getMessage());
      return "{}";
    }
  }
}

