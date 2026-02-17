package com.fanout.transformers;

import com.fanout.model.Record;

/**
 * Converts a {@link Record} into a Protobuf-like mock payload (String) for gRPC sinks.
 *
 * Placeholder format only (not real protobuf).
 */
public class GrpcTransformer implements Transformer<String> {

  public GrpcTransformer() {
    // empty
  }

  @Override
  public String transform(Record record) {
    // Example mock: RecordProto{id="1", name="A", email="a@b.com", timestamp="..."}
    return "RecordProto{"
        + "id=\"" + safe(record.id()) + "\", "
        + "name=\"" + safe(record.name()) + "\", "
        + "email=\"" + safe(record.email()) + "\", "
        + "timestamp=\"" + (record.timestamp() == null ? "" : record.timestamp().toString()) + "\""
        + "}";
  }

  private String safe(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("\"", "\\\"");
  }
}

