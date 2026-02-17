package com.fanout.utils;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Utility for reading {@code application.yaml}.
 *
 * Placeholder: does not implement full YAML resource loading yet, but provides
 * a thin accessor surface so other components can be wired.
 */
public class ConfigLoader {

  private final Map<String, Object> config;

  public ConfigLoader() {
    this(Collections.emptyMap());
  }

  public ConfigLoader(Map<String, Object> config) {
    this.config = Objects.requireNonNull(config, "config");
  }

  /**
   * Placeholder config load method.
   *
   * @return parsed config map (currently empty placeholder)
   */
  public static Map<String, Object> load() {
    // Intentionally not implementing resource/file reading yet.
    // Keeping SnakeYAML dependency wired for when implementation is added.
    Yaml yaml = new Yaml();

    // Placeholder: example of how it might be wired later (not executed).
    InputStream ignored = null;

    return Collections.emptyMap();
  }

  /**
   * Returns the input file path used by {@code FileIngestor}.
   *
   * Placeholder behavior:
   * - Checks config key "inputFilePath" (when you start loading YAML into config).
   * - Falls back to system property "fanout.inputFilePath" if present.
   */
  public String getInputFilePath() {
    Object v = config.get("inputFilePath");
    if (v instanceof String s && !s.isBlank()) {
      return s;
    }
    String fromSysProp = System.getProperty("fanout.inputFilePath");
    if (fromSysProp != null && !fromSysProp.isBlank()) {
      return fromSysProp;
    }
    return null;
  }

  /**
   * Returns the rate limit (records/sec) for a sink type.
   *
   * Lookup order (placeholder):
   * - Config map key: {@code sinks.<sinkTypeLower>.rateLimitRps}
   * - Config map key: {@code rateLimitRps} (global)
   * - System property: {@code fanout.sinks.<sinkTypeLower>.rateLimitRps}
   * - System property: {@code fanout.rateLimitRps} (global)
   *
   * @param sinkType e.g. REST, GRPC, MQ, DB
   * @return records/sec; {@code 0} means "no rate limit"
   */
  public int getRateLimitRps(String sinkType) {
    String type = (sinkType == null) ? "" : sinkType.trim().toLowerCase();

    Object perSink = config.get("sinks." + type + ".rateLimitRps");
    Integer perSinkParsed = asInt(perSink);
    if (perSinkParsed != null && perSinkParsed >= 0) {
      return perSinkParsed;
    }

    Integer globalParsed = asInt(config.get("rateLimitRps"));
    if (globalParsed != null && globalParsed >= 0) {
      return globalParsed;
    }

    String sysPerSink = System.getProperty("fanout.sinks." + type + ".rateLimitRps");
    Integer sysPerSinkParsed = parseInt(sysPerSink);
    if (sysPerSinkParsed != null && sysPerSinkParsed >= 0) {
      return sysPerSinkParsed;
    }

    String sysGlobal = System.getProperty("fanout.rateLimitRps");
    Integer sysGlobalParsed = parseInt(sysGlobal);
    if (sysGlobalParsed != null && sysGlobalParsed >= 0) {
      return sysGlobalParsed;
    }

    return 0;
  }

  private Integer asInt(Object value) {
    if (value instanceof Integer i) {
      return i;
    }
    if (value instanceof Long l) {
      return (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) ? l.intValue() : null;
    }
    if (value instanceof Number n) {
      return n.intValue();
    }
    if (value instanceof String s) {
      return parseInt(s);
    }
    return null;
  }

  private Integer parseInt(String s) {
    if (s == null || s.isBlank()) {
      return null;
    }
    try {
      return Integer.parseInt(s.trim());
    } catch (NumberFormatException ignored) {
      return null;
    }
  }
}

