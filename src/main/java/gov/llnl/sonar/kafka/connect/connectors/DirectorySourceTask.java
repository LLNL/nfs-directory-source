package gov.llnl.sonar.kafka.connect.connectors;

import gov.llnl.sonar.kafka.connect.converters.Converter;
import gov.llnl.sonar.kafka.connect.readers.DirectoryReader;
import gov.llnl.sonar.kafka.connect.readers.RawRecord;
import gov.llnl.sonar.kafka.connect.util.OptionsParser;
import gov.llnl.sonar.kafka.connect.util.VersionUtil;
import io.confluent.connect.avro.AvroData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONObject;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class DirectorySourceTask extends SourceTask {

    private String taskID;
    private static final String PARTITION_FIELD = "filename";
    private static final String OFFSET_FIELD = "line";

    private String topic;
    private DirectoryReader reader = null;
    private org.apache.kafka.connect.data.Schema connectSchema;
    private Function<Object, Object> rawRecordConverter;

    private static final long POLLING_MEMORY_REQUIRED = 8*1000*1000; // 8MB

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

            this.topic = config.getTopic();

            this.reader = new DirectoryReader(
                    relativeDirname,
                    completedDirname,
                    avroSchema,
                    config.getBatchRows(),
                    config.getBatchFiles(),
                    PARTITION_FIELD,
                    OFFSET_FIELD,
                    config.getFormat(),
                    new JSONObject(config.getFormatOptions()),
                    config.getZooKeeperHost(),
                    config.getZooKeeperPort(),
                    config.getEofSentinel());

            AvroData avroData = new AvroData(2);
            this.connectSchema = avroData.toConnectSchema(avroSchema);

            this.rawRecordConverter = Converter.getConverterFor(avroData, connectSchema, config.getFormat());

            log.info("Task {}: Added ingestion directory {}", taskID, reader.getCanonicalDirname());

        } catch (Exception ex) {
            log.error("Task {}: {}", taskID, ex);
            this.stop();
        }
    }

    private long approximateAllocatableMemory() {
        return Runtime.getRuntime().maxMemory() -
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
    }

    @Override
    public synchronized List<SourceRecord> poll() throws InterruptedException {

        ArrayList<RawRecord> rawRecords = new ArrayList<>();

        Long mem;
        if ((mem = approximateAllocatableMemory()) < POLLING_MEMORY_REQUIRED) {
            log.warn("Task {}: Available memory {} less than required amount {}", taskID, mem, POLLING_MEMORY_REQUIRED);
            log.warn("Task {}: Polling paused for 1 second and sending hint to garbage collect", taskID);
            System.gc(); // tell the system to garbage collect soon
            this.wait(1000);
            return null;
        }

        try {
            Long numRecordsRead = reader.read(rawRecords, context);

            if (numRecordsRead > 0) {

                List<SourceRecord> parsedRecords = rawRecords.stream().map(r -> {
                    return r.toSourceRecord(topic, connectSchema, rawRecordConverter);
                }).collect(Collectors.toList());

                log.info("Task {}: Read {} records from directory {}", taskID, numRecordsRead, reader.getCanonicalDirname());

                return parsedRecords;

            }
            else {
                log.debug("Task {}: No records read from {}, sleeping for 1 second", taskID, reader.getCanonicalDirname());
                this.wait(1000);
            }
        } catch (Exception ex) {
            log.error("Task {}: {}", taskID, ex);
            this.wait(1000);
        }

        return null;
    }

    @Override
    public synchronized void stop() {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (Exception ex) {
            log.error("Task {}: {}", taskID, ex);
        }
        this.notify();
    }
}

