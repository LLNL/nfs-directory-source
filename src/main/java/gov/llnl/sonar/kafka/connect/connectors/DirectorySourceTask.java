package gov.llnl.sonar.kafka.connect.connectors;

import gov.llnl.sonar.kafka.connect.readers.DirectoryReader;
import gov.llnl.sonar.kafka.connect.readers.FileReader;
import gov.llnl.sonar.kafka.connect.util.OptionsParser;
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

    private String taskid;
    private static final String PARTITION_FIELD = "filename";
    private static final String OFFSET_FIELD = "line";

    private DirectoryReader reader = null;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {

        taskid = map.get("task.id");

        DirectorySourceConfig config = new DirectorySourceConfig(map);
        try {
            String relativeDirname = config.getDirname();
            String completedDirname = config.getCompletedDirname();

            org.apache.avro.Schema avroSchema;
            if (!config.getAvroSchema().isEmpty()) {
                avroSchema = new org.apache.avro.Schema.Parser().parse(config.getAvroSchema());
            } else {
                avroSchema = new org.apache.avro.Schema.Parser().parse(new File(config.getAvroSchemaFilename()));
            }

            reader = new DirectoryReader(
                    taskid,
                    relativeDirname,
                    completedDirname,
                    config.getTopic(),
                    avroSchema,
                    config.getBatchSize(),
                    PARTITION_FIELD,
                    OFFSET_FIELD,
                    config.getFormat(),
                    OptionsParser.optionsStringToMap(config.getFormatOptions()));

            log.info("Added ingestion directory {}", reader.getCanonicalDirname());

        } catch (Exception ex) {
            log.error("Task {}: Exception:", taskid, ex);
            log.error("Start failed, stopping task!");
            this.stop();
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        ArrayList<SourceRecord> records = new ArrayList<>();

        try {
            Long numRecordsRead = reader.read(records, context);
            if (numRecordsRead > 0) {
                log.info("Task {}: Read {} records from directory {}", taskid, numRecordsRead, reader.getCanonicalDirname());
                return records;
            }
            else {
                log.debug("Task {}: No records read from {}, sleeping for 1 second", taskid, reader.getCanonicalDirname());
                synchronized (this) {
                    this.wait(1000);
                }
            }
        } catch (Exception ex) {
            log.error("Task {}: Exception:", taskid, ex);
            synchronized (this) {
                this.wait(1000);
            }
        }

        return null;
    }

    @Override
    public void stop() {
        log.debug("Task stopping");
        synchronized (this) {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (Exception ex) {
                log.error("Task {}: Exception:", taskid, ex);
            }
            this.notify();
        }
    }
}

