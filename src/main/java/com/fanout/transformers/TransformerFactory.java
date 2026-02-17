package com.fanout.transformers;

/**
 * Factory to select a transformer based on sink type.
 */
public class TransformerFactory {

  public TransformerFactory() {
    // empty
  }

  public Transformer<?> getTransformer(String sinkType) {
    if (sinkType == null) {
      throw new IllegalArgumentException("sinkType must not be null");
    }

    return switch (sinkType.trim().toUpperCase()) {
      case "REST" -> new RestTransformer();
      case "GRPC" -> new GrpcTransformer();
      case "MQ" -> new MqTransformer();
      case "DB" -> new DbTransformer();
      default -> throw new IllegalArgumentException("Unknown sinkType: " + sinkType);
    };
  }
}

