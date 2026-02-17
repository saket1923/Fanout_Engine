package com.fanout.sinks;

import com.fanout.utils.ConfigLoader;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mock REST sink.
 *
 * - Transforms {@link Record} to JSON using {@link RestTransformer}
 * - Simulates HTTP POST by printing payload
 * - Enforces a simple rate limit (records/sec) via {@link Thread#sleep(long)}
 * - Simulates ~10% failures and retries up to 3 attempts
 */
public class RestApiSink implements Sink<String> {

  private static final String SINK_TYPE = "REST";
  private static final int MAX_ATTEMPTS = 3;

  private final int rateLimitRps;

  private final AtomicLong successCount = new AtomicLong();
  private final AtomicLong failureCount = new AtomicLong();
  private final AtomicLong retryCount = new AtomicLong();

  private long nextAllowedNanos = 0L;

  public RestApiSink(ConfigLoader configLoader) {
    Objects.requireNonNull(configLoader, "configLoader");
    this.rateLimitRps = configLoader.getRateLimitRps(SINK_TYPE);
  }

  @Override
  public String sinkType() {
    return SINK_TYPE;
  }

  @Override
  public void send(String payload) {
    Objects.requireNonNull(payload, "payload");
    rateLimitIfNeeded();

    boolean sent = false;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      if (attempt > 1) {
        retryCount.incrementAndGet();
      }

      if (simulateFailure()) {
        System.err.println("RestApiSink: HTTP POST failed (attempt " + attempt + "/" + MAX_ATTEMPTS + ")");
        if (attempt == MAX_ATTEMPTS) {
          failureCount.incrementAndGet();
        } else {
          sleepQuietly(50L * attempt); // tiny backoff
        }
        continue;
      }

      // Simulate POST by printing JSON payload
      System.out.println("RestApiSink: HTTP POST payload=" + payload);
      successCount.incrementAndGet();
      sent = true;
      break;
    }

    if (!sent) {
      System.err.println("RestApiSink: giving up after " + MAX_ATTEMPTS
          + " attempts. successes=" + successCount.get()
          + ", failures=" + failureCount.get()
          + ", retries=" + retryCount.get());
    }
  }

  private boolean simulateFailure() {
    return ThreadLocalRandom.current().nextDouble() < 0.10;
  }

  private void rateLimitIfNeeded() {
    if (rateLimitRps <= 0) {
      return;
    }

    long intervalNanos = 1_000_000_000L / Math.max(1, rateLimitRps);
    long sleepNanos;
    long now = System.nanoTime();

    synchronized (this) {
      if (nextAllowedNanos == 0L) {
        nextAllowedNanos = now;
      }
      sleepNanos = nextAllowedNanos - now;
      nextAllowedNanos = Math.max(nextAllowedNanos, now) + intervalNanos;
    }

    if (sleepNanos > 0) {
      sleepNanos(sleepNanos);
    }
  }

  private void sleepNanos(long nanos) {
    long ms = nanos / 1_000_000L;
    int ns = (int) (nanos % 1_000_000L);
    try {
      Thread.sleep(ms, ns);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  private void sleepQuietly(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }
}

