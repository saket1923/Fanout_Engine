package com.fanout.transformers;

import com.fanout.model.Record;

/**
 * Transforms an incoming {@link Record} into a sink-specific payload.
 *
 * @param <T> transformed payload type (e.g. String, Map, etc.)
 */
public interface Transformer<T> {

  T transform(Record record);
}

