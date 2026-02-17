package com.fanout.model;

import java.time.Instant;

/**
 * Simple data model placeholder.
 *
 * Note: name intentionally kept as "Record" per project spec.
 */
public record Record(
    String id,
    String name,
    String email,
    Instant timestamp
) {
  // Placeholder for validation or normalization logic later.
  public Record {
    // no-op
  }
}

