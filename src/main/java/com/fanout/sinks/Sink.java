package com.fanout.sinks;

/**
 * Sends a transformed payload to an external destination.
 *
 * @param <T> payload type (e.g. String, Map, etc.)
 */
public interface Sink<T> {

  /**
   * Sink type identifier used for transformer selection and metrics.
   * Example values: REST, GRPC, MQ, DB
   */
  String sinkType();

  void send(T payload);
}

