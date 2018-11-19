package gov.llnl.sonar.kafka.connect.parsers;

import io.confluent.connect.avro.AvroData;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileStreamParserBuilder {

    // Defaults
    private String format = "csv";
    private JSONObject formatOptions = new JSONObject();
    private int bufferSize = 8192;

    // Optional
    private String eofSentinel;

    // Must be set
    private org.apache.avro.Schema avroSchema;
    private String partitionField;
    private String offsetField;

    // Generated internally
    private AvroData avroData;
    private org.apache.kafka.connect.data.Schema connectSchema;

    public FileStreamParserBuilder() {
        this.avroData = new AvroData(2);
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public void setFormatOptions(JSONObject formatOptions) {
        this.formatOptions = formatOptions;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setEofSentinel(String eofSentinel) {
        this.eofSentinel = eofSentinel;
    }

    public void setAvroSchema(org.apache.avro.Schema avroSchema) {
        this.avroSchema = avroSchema;
        this.connectSchema = avroData.toConnectSchema(avroSchema);
    }

    public void setPartitionField(String partitionField) {
        this.partitionField = partitionField;
    }

    public void setOffsetField(String offsetField) {
        this.offsetField = offsetField;
    }

    public FileStreamParser build(String fileName) throws IllegalArgumentException, IOException {
        return build(Paths.get(fileName));
    }

    public FileStreamParser build(Path filePath) throws IllegalArgumentException, IOException {

        if (avroSchema == null) {
            throw new IllegalArgumentException("FileStreamParserBuilder requires an avroSchema!");
        }

        switch (format) {
            case "csv":
                return new CsvFileStreamParser(
                        filePath,
                        formatOptions,
                        avroData,
                        avroSchema,
                        connectSchema,
                        eofSentinel,
                        bufferSize,
                        0L,
                        partitionField,
                        offsetField);
            case "json":
                return new JsonFileStreamParser(
                        filePath,
                        formatOptions,
                        avroData,
                        avroSchema,
                        connectSchema,
                        eofSentinel,
                        bufferSize,
                        0L,
                        partitionField,
                        offsetField);
            default:
                throw new IllegalArgumentException("Invalid file format " + format);
        }
    }
}
