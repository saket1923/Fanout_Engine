package com.fanout.transformers;

import com.fanout.model.Record;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Converts a {@link Record} into a mock "Avro/CQL-like" map for DB sinks.
 *
 * Placeholder only: this is not real Avro or CQL.
 */
public class DbTransformer implements Transformer<Map<String, Object>> {

  public DbTransformer() {
    // empty
  }

  @Override
  public Map<String, Object> transform(Record record) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("id", record.id());
    out.put("name", record.name());
    out.put("email", record.email());
    out.put("timestamp", record.timestamp() == null ? null : record.timestamp().toString());
    return out;
  }
}

