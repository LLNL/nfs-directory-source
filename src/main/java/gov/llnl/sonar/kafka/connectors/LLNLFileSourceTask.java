package gov.llnl.sonar.kafka.connectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// TODO: handle file not found
// TODO: multiple tasks, thread safety

public class LLNLFileSourceTask extends SourceTask implements Loggable {
    private static final String PARTITION_FIELD = "filename";
    private static final String OFFSET_FIELD = "position";

    private ConnectFileReader reader;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        LLNLFileSourceConfig config = new LLNLFileSourceConfig(map);
        try {

            String relativeFilename = config.getFilename();

            org.apache.avro.Schema avroSchema;
            if (!config.getAvroSchema().isEmpty()) {
                avroSchema = new org.apache.avro.Schema.Parser().parse(config.getAvroSchema());
            } else {
                avroSchema = new org.apache.avro.Schema.Parser().parse(new File(config.getAvroSchemaFilename()));
            }

            reader = new ConnectFileReader(
                    relativeFilename,
                    config.getTopic(),
                    avroSchema,
                    config.getBatchSize(),
                    PARTITION_FIELD,
                    OFFSET_FIELD);

        } catch (Exception ex) {
            log.error(TAG, ex);
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

        } catch (Exception e) {
            log.error(TAG, e);
        }
        return null;
    }

    @Override
    public void stop() {
        log.info(TAG + "Task stopping");
        try {
            reader.close();
        } catch (Exception ex) {
            log.error(TAG, ex);
        }
    }
}