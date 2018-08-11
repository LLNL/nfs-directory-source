package gov.llnl.sonar.kafka.connect.connectors;

import gov.llnl.sonar.kafka.connect.converters.Converter;
import gov.llnl.sonar.kafka.connect.converters.CsvRecordConverter;
import gov.llnl.sonar.kafka.connect.readers.FileReader;
import gov.llnl.sonar.kafka.connect.readers.RawRecord;
import gov.llnl.sonar.kafka.connect.util.VersionUtil;
import gov.llnl.sonar.kafka.connect.util.OptionsParser;
import io.confluent.connect.avro.AvroData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONObject;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class FileSourceTask extends SourceTask {
    private String taskID;
    private static final String PARTITION_FIELD = "filename";
    private static final String OFFSET_FIELD = "line";

    private String topic;
    private FileReader reader;
    private org.apache.kafka.connect.data.Schema connectSchema;
    private Function<Object, Object> rawRecordConverter;

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

            this.topic = config.getTopic();

            this.reader = new FileReader(
                    relativeFilename,
                    config.getCompletedDirname(),
                    avroSchema,
                    config.getBatchSize(),
                    PARTITION_FIELD,
                    OFFSET_FIELD,
                    config.getFormat(),
                    new JSONObject(config.getFormatOptions()),
                    0L,
                    config.getEofSentinel());

            AvroData avroData = new AvroData(2);
            this.connectSchema = avroData.toConnectSchema(avroSchema);

            this.rawRecordConverter = Converter.getConverterFor(avroData, connectSchema, config.getFormat());

        } catch (Exception ex) {
            log.error("Task {}: {}", taskID, ex);
        }
    }

    @Override
    public List<SourceRecord> poll() {

        ArrayList<RawRecord> rawRecords = new ArrayList<>();

        try {

            reader.read(rawRecords, context);

            if (rawRecords.isEmpty())
                return null;

            List<SourceRecord> parsedRecords = rawRecords.stream().map((RawRecord r) ->
                    r.toSourceRecord(topic, connectSchema, rawRecordConverter))
                .collect(Collectors.toList());

            log.info("Task {}: Read {} records from file {}", taskID, rawRecords.size(), reader.getPath());

            return parsedRecords;

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