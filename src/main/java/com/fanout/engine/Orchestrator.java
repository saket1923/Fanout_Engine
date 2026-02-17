package com.fanout.engine;

import com.fanout.model.Record;
import com.fanout.sinks.GrpcSink;
import com.fanout.sinks.MessageQueueSink;
import com.fanout.sinks.RestApiSink;
import com.fanout.sinks.Sink;
import com.fanout.sinks.WideColumnDbSink;
import com.fanout.transformers.Transformer;
import com.fanout.transformers.TransformerFactory;
import com.fanout.utils.ConfigLoader;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main engine entrypoint that orchestrates transformation + fanout to sinks.
 */
public class Orchestrator {

  private final BlockingQueue<Record> queue;
  private final List<Sink<?>> sinks;
  private final FileIngestor ingestor;
  private final ExecutorService executor;
  private final ScheduledExecutorService metricsExecutor;
  private final TransformerFactory transformerFactory;

  private final AtomicLong processed = new AtomicLong();
  private final ConcurrentHashMap<String, AtomicLong> successBySink = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AtomicLong> failureBySink = new ConcurrentHashMap<>();

  public Orchestrator(BlockingQueue<Record> queue,
                      List<Sink<?>> sinks,
                      FileIngestor ingestor) {
    this.queue = Objects.requireNonNull(queue, "queue");
    this.sinks = new ArrayList<>(Objects.requireNonNull(sinks, "sinks"));
    this.ingestor = Objects.requireNonNull(ingestor, "ingestor");
    this.executor = Executors.newVirtualThreadPerTaskExecutor();
    this.metricsExecutor = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("metrics", 0).factory());
    this.transformerFactory = new TransformerFactory();

    for (Sink<?> sink : this.sinks) {
      successBySink.putIfAbsent(sink.sinkType(), new AtomicLong());
      failureBySink.putIfAbsent(sink.sinkType(), new AtomicLong());
    }
  }

  public Orchestrator() {
    ConfigLoader configLoader = new ConfigLoader(ConfigLoader.load());

    this.queue = new ArrayBlockingQueue<>(10_000);
    this.sinks = List.of(
        new RestApiSink(configLoader),
        new GrpcSink(configLoader),
        new MessageQueueSink(configLoader),
        new WideColumnDbSink(configLoader)
    );
    this.ingestor = new FileIngestor(queue, configLoader);
    this.executor = Executors.newVirtualThreadPerTaskExecutor();
    this.metricsExecutor = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("metrics", 0).factory());
    this.transformerFactory = new TransformerFactory();

    for (Sink<?> sink : this.sinks) {
      successBySink.putIfAbsent(sink.sinkType(), new AtomicLong());
      failureBySink.putIfAbsent(sink.sinkType(), new AtomicLong());
    }
  }

  public void run() {
    AtomicBoolean ingestionDone = new AtomicBoolean(false);
    List<Future<?>> consumerFutures = new ArrayList<>();

    // Metrics every 5 seconds
    AtomicLong lastProcessed = new AtomicLong(0);
    AtomicLong lastTimeNanos = new AtomicLong(System.nanoTime());
    metricsExecutor.scheduleAtFixedRate(() -> {
      long now = System.nanoTime();
      long total = processed.get();
      long prev = lastProcessed.getAndSet(total);
      long prevTime = lastTimeNanos.getAndSet(now);
      double seconds = (now - prevTime) / 1_000_000_000.0;
      double throughput = seconds > 0 ? (total - prev) / seconds : 0.0;

      System.out.println("Metrics: processed=" + total
          + ", throughput=" + String.format("%.2f", throughput) + " rec/s"
          + ", successBySink=" + snapshot(successBySink)
          + ", failureBySink=" + snapshot(failureBySink));
    }, 5, 5, TimeUnit.SECONDS);

    // Start ingestion in its own thread
    Thread ingestThread = Thread.ofVirtual().name("file-ingestor").start(() -> {
      try {
        ingestor.ingestFile();
      } catch (Exception e) {
        System.err.println("Orchestrator: ingestor failed: " + e.getMessage());
      } finally {
        ingestionDone.set(true);
      }
    });

    // Start consumers (virtual threads)
    int consumerCount = Math.max(1, Runtime.getRuntime().availableProcessors());
    for (int i = 0; i < consumerCount; i++) {
      consumerFutures.add(executor.submit(() -> consumeLoop(ingestionDone)));
    }

    // Graceful shutdown: wait for ingestion, then for queue drain, then consumers to exit
    try {
      ingestThread.join();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      System.err.println("Orchestrator: interrupted while waiting for ingestor.");
    }

    while (!queue.isEmpty()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    for (Future<?> f : consumerFutures) {
      try {
        f.get();
      } catch (Exception e) {
        System.err.println("Orchestrator: consumer exited with error: " + e.getMessage());
      }
    }

    metricsExecutor.shutdownNow();
    executor.shutdown();
    try {
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }

    printFinalSummary();
  }

  private void consumeLoop(AtomicBoolean ingestionDone) {
    while (true) {
      try {
        Record record = queue.poll(200, TimeUnit.MILLISECONDS);
        if (record == null) {
          if (ingestionDone.get() && queue.isEmpty()) {
            return;
          }
          continue;
        }

        processed.incrementAndGet();

        for (Sink<?> sink : sinks) {
          String sinkType = sink.sinkType();
          try {
            Transformer<?> transformer = transformerFactory.getTransformer(sinkType);
            Object payload = transformUnchecked(transformer, record);
            sendUnchecked(sink, payload);
            successBySink.computeIfAbsent(sinkType, k -> new AtomicLong()).incrementAndGet();
          } catch (Exception e) {
            failureBySink.computeIfAbsent(sinkType, k -> new AtomicLong()).incrementAndGet();
            System.err.println("Orchestrator: sink=" + sinkType + " failed: " + e.getMessage());
          }
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return;
      } catch (Exception e) {
        System.err.println("Orchestrator: consumer loop error: " + e.getMessage());
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Object transformUnchecked(Transformer<?> transformer, Record record) {
    return ((Transformer<Object>) transformer).transform(record);
  }

  @SuppressWarnings("unchecked")
  private void sendUnchecked(Sink<?> sink, Object payload) {
    ((Sink<Object>) sink).send(payload);
  }

  private String snapshot(ConcurrentHashMap<String, AtomicLong> map) {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (var e : map.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      sb.append(e.getKey()).append("=").append(e.getValue().get());
    }
    sb.append("}");
    return sb.toString();
  }

  private void printFinalSummary() {
    System.out.println("Final summary: processed=" + processed.get()
        + ", successBySink=" + snapshot(successBySink)
        + ", failureBySink=" + snapshot(failureBySink));
  }

  public static void main(String[] args) {
    new Orchestrator().run();
  }
}

