package com.fanout.transformers;

import com.fanout.model.Record;

/**
 * Converts a {@link Record} into a simple XML payload (String) for MQ sinks.
 *
 * Placeholder only: minimal manual XML without namespaces/schema.
 */
public class MqTransformer implements Transformer<String> {

  public MqTransformer() {
    // empty
  }

  @Override
  public String transform(Record record) {
    StringBuilder sb = new StringBuilder();
    sb.append("<record>");
    sb.append("<id>").append(xmlEscape(record.id())).append("</id>");
    sb.append("<name>").append(xmlEscape(record.name())).append("</name>");
    sb.append("<email>").append(xmlEscape(record.email())).append("</email>");
    sb.append("<timestamp>").append(record.timestamp() == null ? "" : xmlEscape(record.timestamp().toString())).append("</timestamp>");
    sb.append("</record>");
    return sb.toString();
  }

  private String xmlEscape(String s) {
    if (s == null) {
      return "";
    }
    return s
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&apos;");
  }
}

