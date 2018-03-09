package gov.llnl.sonar.kafka.connectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LLNLDirectorySourceTask extends SourceTask implements Loggable {

    private static final String PARTITION_FIELD = "filename";
    private static final String OFFSET_FIELD = "position";

    private ConnectDirectoryReader reader;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        LLNLDirectorySourceConfig config = new LLNLDirectorySourceConfig(map);
        try {
            String relativeDirname = config.getDirname();

            org.apache.avro.Schema avroSchema;
            if (!config.getAvroSchema().isEmpty()) {
                avroSchema = new org.apache.avro.Schema.Parser().parse(config.getAvroSchema());
            } else {
                avroSchema = new org.apache.avro.Schema.Parser().parse(new File(config.getAvroSchemaFilename()));
            }

            reader = new ConnectDirectoryReader(relativeDirname,
                                                config.getTopic(),
                                                avroSchema,
                                                config.getBatchSize(),
                                                PARTITION_FIELD,
                                                OFFSET_FIELD);

            log.info(TAG + "Added directory {}", reader.getCanonicalDirname());

        } catch (Exception ex) {
            log.error(TAG, ex);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        ArrayList<SourceRecord> records = new ArrayList<>();

        try {
            Long numRecordsRead = reader.read(records, context);
            log.info(TAG + "Read {} records from directory {}", numRecordsRead, reader.getCanonicalDirname());
            return records;
        } catch (Exception ex) {
            log.error(TAG, ex);
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

