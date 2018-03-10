package gov.llnl.sonar.kafka.connect.connectors;

import gov.llnl.sonar.kafka.connect.readers.FileReader;
import gov.llnl.sonar.kafka.connect.util.VersionUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// TODO: handle file not found
// TODO: multiple tasks, thread safety

@Slf4j
public class FileSourceTask extends SourceTask {
    private static final String PARTITION_FIELD = "filename";
    private static final String OFFSET_FIELD = "position";

    private FileReader reader;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        FileSourceConfig config = new FileSourceConfig(map);
        try {

            String relativeFilename = config.getFilename();

            org.apache.avro.Schema avroSchema;
            if (!config.getAvroSchema().isEmpty()) {
                avroSchema = new org.apache.avro.Schema.Parser().parse(config.getAvroSchema());
            } else {
                avroSchema = new org.apache.avro.Schema.Parser().parse(new File(config.getAvroSchemaFilename()));
            }

            reader = new FileReader(
                    relativeFilename,
                    config.getTopic(),
                    avroSchema,
                    config.getBatchSize(),
                    PARTITION_FIELD,
                    OFFSET_FIELD,
                    config.getFormat());

        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        ArrayList<SourceRecord> records = new ArrayList<>();

        try {

            reader.read(records, context);

            if (records.isEmpty())
                return null;

            return records;

        } catch (Exception ex) {
            log.error("Exception: ", ex);
        }
        return null;
    }

    @Override
    public void stop() {
        log.info("Task stopping");
        try {
            reader.close();
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
    }
}