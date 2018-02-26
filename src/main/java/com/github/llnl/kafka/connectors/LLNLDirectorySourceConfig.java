package com.github.llnl.kafka.connectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public class LLNLDirectorySourceConfig extends AbstractConfig {

    public LLNLDirectorySourceConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }
    public LLNLDirectorySourceConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    private static final String DIRNAME = "dirname";
    private static final String DIRNAME_DOC = "The name of the directory to read from.";
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
    private static final String BATCH_SIZE = "batch.size";
    private static final String BATCH_SIZE_DOC = "Number of lines to read/ingest at a time from the file.";

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(DIRNAME, Type.STRING, Importance.HIGH, DIRNAME_DOC)
                .define(TOPIC, Type.STRING, Importance.HIGH, TOPIC_DOC)
                .define(FORMAT, Type.STRING, Importance.HIGH, FORMAT_DOC)
                .define(FORMAT_OPTIONS, Type.STRING, "", Importance.LOW, FORMAT_OPTIONS_DOC)
                .define(AVRO_SCHEMA, Type.STRING, "", Importance.HIGH, AVRO_SCHEMA_DOC)
                .define(AVRO_SCHEMA_FILENAME, Type.STRING, "", Importance.HIGH, AVRO_SCHEMA_FILENAME_DOC)
                .define(BATCH_SIZE, Type.LONG, 1000L, Importance.HIGH, BATCH_SIZE_DOC)
                ;
    }

    public String getDirname() { return this.getString(DIRNAME); }
    public String getTopic() { return this.getString(TOPIC); }
    public String getAvroSchema() { return this.getString(AVRO_SCHEMA); }
    public String getAvroSchemaFilename() { return this.getString(AVRO_SCHEMA_FILENAME); }
    public Long getBatchSize() { return this.getLong(BATCH_SIZE); }
}

