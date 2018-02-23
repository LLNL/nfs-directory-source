package com.github.llnl.kafka.connectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;


public class LLNLFileSourceConfig extends AbstractConfig {

    public LLNLFileSourceConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }
    public LLNLFileSourceConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    private static final String FILENAME = "filename";
    private static final String FILENAME_DOC = "The name of the file to read from.";
    private static final String TOPIC = "topic";
    private static final String TOPIC_DOC = "The name of the topic to stream to.";
    private static final String FORMAT = "format";
    private static final String FORMAT_DOC = "Format of the file [ csv | json ]";
    private static final String FORMAT_OPTIONS = "format.options";
    private static final String FORMAT_OPTIONS_DOC = "Comma-separated list of formatting options as option:value.\n" +
            "Available options:\n" +
            "   csv: header:[true|false],delim:<char>,quote=<char>\n" +
            "   json: orient:[records]" ;
    private static final String AVRO_SCHEMA = "avro.schema";
    private static final String AVRO_SCHEMA_DOC = "Avro schema string, e.g., " +
            "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"idstr\",\n" +
            "  \"fields\" : [\n" +
            "    {\"name\": \"id\", \"type\": \"int\"},\n" +
            "    {\"name\": \"str\", \"type\": \"string\"},\n" +
            "  ]\n" +
            "}";
    private static final String AVRO_SCHEMA_FILENAME = "avro.schema.filename";
    private static final String AVRO_SCHEMA_FILENAME_DOC = "Avro schema filename.";

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(FILENAME, Type.STRING, Importance.HIGH, FILENAME_DOC)
                .define(TOPIC, Type.STRING, Importance.HIGH, TOPIC_DOC)
                .define(FORMAT, Type.STRING, Importance.HIGH, FORMAT_DOC)
                .define(FORMAT_OPTIONS, Type.STRING, "", Importance.LOW, FORMAT_OPTIONS_DOC)
                .define(AVRO_SCHEMA, Type.STRING, "", Importance.HIGH, AVRO_SCHEMA_DOC)
                .define(AVRO_SCHEMA_FILENAME, Type.STRING, "", Importance.HIGH, AVRO_SCHEMA_FILENAME_DOC)
                ;
    }

    public String getFilename() { return this.getString(FILENAME); }
    public String getTopic() { return this.getString(TOPIC); }
    public String getAvroSchema() { return this.getString(AVRO_SCHEMA); }
    public String getAvroSchemaFilename() { return this.getString(AVRO_SCHEMA_FILENAME); }
}

