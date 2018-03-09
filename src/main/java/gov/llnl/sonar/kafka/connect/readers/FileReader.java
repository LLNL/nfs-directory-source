package gov.llnl.sonar.kafka.connect.readers;

import gov.llnl.sonar.kafka.connect.parsers.AvroFileStreamParser;
import gov.llnl.sonar.kafka.connect.util.ConnectUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public class FileReader extends AbstractReader {
    private String canonicalFilename;
    private Path canonicalPath;
    private String topic;

    private Long batchSize;
    private String partitionField;
    private String offsetField;

    private AvroFileStreamParser avroStreamParser;

    public FileReader(String filename,
                      String topic,
                      org.apache.avro.Schema avroSchema,
                      Long batchSize,
                      String partitionField,
                      String offsetField) {

        this.topic = topic;
        this.batchSize = batchSize;
        this.partitionField = partitionField;
        this.offsetField = offsetField;

        try {
            File file = new File(filename);
            this.canonicalPath = file.toPath().toRealPath();
            this.canonicalFilename = file.getCanonicalPath();

            this.avroStreamParser = new AvroFileStreamParser(canonicalFilename, avroSchema);

        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
    }

    @Override
    public Long read(List<SourceRecord> records, SourceTaskContext context) {
        Long i, offset = ConnectUtil.getStreamOffset(context, partitionField, offsetField, canonicalFilename);
        for (i = 0L; i < batchSize; i++) {

            if (breakAndClose.get())
                break;

            // TODO: filestream may be closed here, fix!

            try {
                Object parsedValue = avroStreamParser.read();

                if (parsedValue != null) {

                    Map sourcePartition = Collections.singletonMap(partitionField, canonicalFilename);
                    Map sourceOffset = Collections.singletonMap(offsetField, offset);

                    records.add(new SourceRecord(sourcePartition, sourceOffset, topic, avroStreamParser.connectSchema, parsedValue));
                    offset++;

                }

            } catch (EOFException e) {
                try {
                    log.info("Purging ingested file {}", canonicalFilename);
                    Files.delete(canonicalPath);
                    close();
                } catch (IOException e1) {
                    log.error("Error deleting file {}", canonicalFilename);
                }
            }

        }
        return i;
    }

    String getCanonicalFilename() {
        return canonicalFilename;
    }

    @Override
    public void close() {
        super.close();
        try {
            avroStreamParser.close();
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
    }
}
