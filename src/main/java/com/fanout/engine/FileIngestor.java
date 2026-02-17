package com.fanout.engine;

import com.fanout.model.Record;
import com.fanout.utils.ConfigLoader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

/**
 * Streams an input CSV or JSONL file and enqueues {@link Record} instances.
 *
 * Notes:
 * - Does not load the whole file into memory.
 * - Minimal parsing only (placeholder).
 */
public class FileIngestor {

  private final BlockingQueue<Record> queue;
  private final String inputFilePath;

  private final ObjectMapper objectMapper = new ObjectMapper();

  public FileIngestor(BlockingQueue<Record> queue, ConfigLoader configLoader) {
    this.queue = Objects.requireNonNull(queue, "queue");
    Objects.requireNonNull(configLoader, "configLoader");
    this.inputFilePath = configLoader.getInputFilePath();
  }

  /**
   * Reads the input file line-by-line, converts each line to a {@link Record},
   * and pushes it into the queue.
   */
  public void ingestFile() {
    if (inputFilePath == null || inputFilePath.isBlank()) {
      System.err.println("FileIngestor: inputFilePath is not configured.");
      return;
    }

    Path path = Path.of(inputFilePath);
    boolean isJsonl = looksLikeJsonl(path);
    boolean isCsv = looksLikeCsv(path);

    long ingested = 0;

    try (BufferedReader reader = Files.newBufferedReader(path)) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isBlank()) {
          continue;
        }

        Record record;
        try {
          if (isJsonl || (!isCsv && looksLikeJsonLine(line))) {
            record = parseJsonLine(line);
          } else {
            record = parseCsvLine(line);
          }
        } catch (Exception parseEx) {
          // Placeholder: skip malformed lines for now.
          System.err.println("FileIngestor: skipping malformed line: " + parseEx.getMessage());
          continue;
        }

        try {
          queue.put(record);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          System.err.println("FileIngestor: interrupted while enqueuing; stopping ingestion.");
          return;
        }

        ingested++;
        if (ingested % 1000 == 0) {
          System.out.println("FileIngestor: ingested " + ingested + " records");
        }
      }
    } catch (IOException ioe) {
      System.err.println("FileIngestor: I/O error while reading '" + inputFilePath + "': " + ioe.getMessage());
    }
  }

  private boolean looksLikeCsv(Path path) {
    String name = path.getFileName() == null ? "" : path.getFileName().toString().toLowerCase();
    return name.endsWith(".csv");
  }

  private boolean looksLikeJsonl(Path path) {
    String name = path.getFileName() == null ? "" : path.getFileName().toString().toLowerCase();
    return name.endsWith(".jsonl") || name.endsWith(".ndjson");
  }

  private boolean looksLikeJsonLine(String line) {
    String t = line.trim();
    return t.startsWith("{") && t.endsWith("}");
  }

  /**
   * Very simple CSV parsing: {@code id,name,email,timestamp}.
   * Timestamp should be ISO-8601 (e.g. {@code 2026-02-17T10:00:00Z}).
   */
  private Record parseCsvLine(String line) {
    String[] parts = line.split(",", -1);
    String id = parts.length > 0 ? blankToNull(parts[0]) : null;
    String name = parts.length > 1 ? blankToNull(parts[1]) : null;
    String email = parts.length > 2 ? blankToNull(parts[2]) : null;
    Instant timestamp = parts.length > 3 ? parseInstantOrNull(parts[3]) : null;

    return new Record(id, name, email, timestamp);
  }

  /**
   * Minimal JSONL parsing expecting keys: {@code id,name,email,timestamp}.
   * Timestamp should be ISO-8601.
   */
  private Record parseJsonLine(String line) throws IOException {
    JsonNode node = objectMapper.readTree(line);

    String id = textOrNull(node.get("id"));
    String name = textOrNull(node.get("name"));
    String email = textOrNull(node.get("email"));
    Instant timestamp = parseInstantOrNull(textOrNull(node.get("timestamp")));

    return new Record(id, name, email, timestamp);
  }

  private String textOrNull(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    String s = node.asText();
    return (s == null || s.isBlank()) ? null : s;
  }

  private String blankToNull(String s) {
    if (s == null) {
      return null;
    }
    String t = s.trim();
    return t.isBlank() ? null : t;
  }

  private Instant parseInstantOrNull(String s) {
    if (s == null || s.isBlank()) {
      return null;
    }
    try {
      return Instant.parse(s.trim());
    } catch (DateTimeParseException ignored) {
      return null;
    }
  }
}

