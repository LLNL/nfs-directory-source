package gov.llnl.sonar.kafka.connect.connectors;

import gov.llnl.sonar.kafka.connect.readers.FileReader;
import gov.llnl.sonar.kafka.connect.util.VersionUtil;
import gov.llnl.sonar.kafka.connect.util.OptionsParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class FileSourceTask extends SourceTask {
    private String taskID;
    private static final String PARTITION_FIELD = "filename";
    private static final String OFFSET_FIELD = "line";

    private FileReader reader;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {

        FileSourceConfig config = new FileSourceConfig(map);
        try {
            this.taskID = InetAddress.getLocalHost().getHostName() + "(" + Thread.currentThread().getId() + ")";

            String relativeFilename = config.getFilename();

            org.apache.avro.Schema avroSchema;
            if (!config.getAvroSchema().isEmpty()) {
                avroSchema = new org.apache.avro.Schema.Parser().parse(config.getAvroSchema());
            } else {
                avroSchema = new org.apache.avro.Schema.Parser().parse(new File(config.getAvroSchemaFilename()));
            }

            // TODO: get offset from zookeeper
            reader = new FileReader(
                    relativeFilename,
                    config.getCompletedDirname(),
                    config.getTopic(),
                    avroSchema,
                    config.getBatchSize(),
                    PARTITION_FIELD,
                    OFFSET_FIELD,
                    config.getFormat(),
                    OptionsParser.optionsStringToMap(config.getFormat()),
                    0L);

        } catch (Exception ex) {
            log.error("Task {}: {}", taskID, ex);
        }
    }

    @Override
    public List<SourceRecord> poll() {

        ArrayList<SourceRecord> records = new ArrayList<>();

        try {

            reader.read(records, context);

            if (records.isEmpty())
                return null;

            return records;

        } catch (Exception ex) {
            log.error("Task {}: {}", taskID, ex);
        }
        return null;
    }

    @Override
    public void stop() {
        log.info("Task {}: Stopping", taskID);
        try {
            reader.close();
        } catch (Exception ex) {
            log.error("Task {}: {}", taskID, ex);
        }
        reader.notify();
    }
}