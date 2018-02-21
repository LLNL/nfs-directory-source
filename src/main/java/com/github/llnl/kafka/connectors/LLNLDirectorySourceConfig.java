package com.github.llnl.kafka.connectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;


public class LLNLDirectorySourceConfig extends AbstractConfig {

  private static final String FILENAME = "filename";
  private static final String FILENAME_DOC = "The name of the file to read from.";
  private static final String TOPIC = "topic";
  private static final String TOPIC_DOC = "The name of the topic to stream to.";

  public LLNLDirectorySourceConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public LLNLDirectorySourceConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
      return new ConfigDef()
            .define(FILENAME, Type.STRING, Importance.HIGH, FILENAME_DOC)
            .define(TOPIC, Type.STRING, Importance.HIGH, TOPIC_DOC);
  }

  public String getFilename() { return this.getString(FILENAME); }

  public String getTopic() { return this.getString(TOPIC); }
}

