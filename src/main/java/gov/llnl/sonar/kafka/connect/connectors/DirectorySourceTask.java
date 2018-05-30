package gov.llnl.sonar.kafka.connect.connectors;

import gov.llnl.sonar.kafka.connect.readers.DirectoryReader;
import gov.llnl.sonar.kafka.connect.util.OptionsParser;
import gov.llnl.sonar.kafka.connect.util.VersionUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class DirectorySourceTask extends SourceTask {

    private String taskID;
    private static final String PARTITION_FIELD = "filename";
    private static final String OFFSET_FIELD = "line";

    private DirectoryReader reader = null;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {


        DirectorySourceConfig config = new DirectorySourceConfig(map);
        try {
            this.taskID = InetAddress.getLocalHost().getHostName() + "(" + Thread.currentThread().getId() + ")";

            String relativeDirname = config.getDirname();
            String completedDirname = config.getCompletedDirname();

            org.apache.avro.Schema avroSchema;
            if (!config.getAvroSchema().isEmpty()) {
                avroSchema = new org.apache.avro.Schema.Parser().parse(config.getAvroSchema());
            } else {
                avroSchema = new org.apache.avro.Schema.Parser().parse(new File(config.getAvroSchemaFilename()));
            }

            reader = new DirectoryReader(
                    relativeDirname,
                    completedDirname,
                    config.getTopic(),
                    avroSchema,
                    config.getBatchSize(),
                    PARTITION_FIELD,
                    OFFSET_FIELD,
                    config.getFormat(),
                    OptionsParser.optionsStringToMap(config.getFormatOptions()),
                    config.getZooKeeperHost(),
                    config.getZooKeeperPort());

            log.info("Task {}: Added ingestion directory {}", taskID, reader.getCanonicalDirname());

        } catch (Exception ex) {
            log.error("Task {}: Exception:", taskID, ex);
            this.stop();
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        ArrayList<SourceRecord> records = new ArrayList<>();

        try {
            Long numRecordsRead = reader.read(records, context);
            if (numRecordsRead > 0) {
                log.info("Task {}: Read {} records from directory {}", taskID, numRecordsRead, reader.getCanonicalDirname());
                return records;
            }
            else {
                log.debug("Task {}: No records read from {}, sleeping for 1 second", taskID, reader.getCanonicalDirname());
                synchronized (this) {
                    this.wait(1000);
                }
            }
        } catch (Exception ex) {
            log.error("Task {}: Exception:", taskID, ex);
            synchronized (this) {
                this.wait(1000);
            }
        }

        return null;
    }

    @Override
    public void stop() {
        synchronized (this) {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (Exception ex) {
                log.error("Task {}: Exception:", taskID, ex);
            }
            this.notify();
        }
    }
}

