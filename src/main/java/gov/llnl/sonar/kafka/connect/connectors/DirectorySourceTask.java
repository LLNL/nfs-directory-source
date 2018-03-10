package gov.llnl.sonar.kafka.connect.connectors;

import gov.llnl.sonar.kafka.connect.readers.DirectoryReader;
import gov.llnl.sonar.kafka.connect.readers.FileReader;
import gov.llnl.sonar.kafka.connect.util.VersionUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class DirectorySourceTask extends SourceTask {

    private static final String PARTITION_FIELD = "filename";
    private static final String OFFSET_FIELD = "position";

    private DirectoryReader reader;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        DirectorySourceConfig config = new DirectorySourceConfig(map);
        try {
            String relativeDirname = config.getDirname();

            org.apache.avro.Schema avroSchema;
            if (!config.getAvroSchema().isEmpty()) {
                avroSchema = new org.apache.avro.Schema.Parser().parse(config.getAvroSchema());
            } else {
                avroSchema = new org.apache.avro.Schema.Parser().parse(new File(config.getAvroSchemaFilename()));
            }

            reader = new DirectoryReader(relativeDirname,
                    config.getTopic(),
                    avroSchema,
                    config.getBatchSize(),
                    PARTITION_FIELD,
                    OFFSET_FIELD,
                    config.getFormat());

            log.info("Added directory {}", reader.getCanonicalDirname());

        } catch (Exception ex) {
            log.error("Exception:", ex);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        ArrayList<SourceRecord> records = new ArrayList<>();

        try {
            Long numRecordsRead = reader.read(records, context);
            log.info("Read {} records from directory {}", numRecordsRead, reader.getCanonicalDirname());
            return records;
        } catch (Exception ex) {
            log.error("Exception:", ex);
        }

        return null;
    }

    @Override
    public void stop() {
        log.info("Task stopping");
        try {
            reader.close();
        } catch (Exception ex) {
            log.error("Exception:", ex);
        }
    }
}

