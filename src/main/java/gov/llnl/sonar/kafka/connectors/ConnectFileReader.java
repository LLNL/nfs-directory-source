package gov.llnl.sonar.kafka.connectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
class ConnectFileReader extends ConnectReader {
    private String canonicalFilename;
    private Path canonicalPath;
    private String topic;

    private Long batchSize;
    private String partitionField;
    private String offsetField;

    private AvroConnectFileStreamParser avroStreamParser;

    ConnectFileReader(String filename,
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

            this.avroStreamParser = new AvroConnectFileStreamParser(canonicalFilename, avroSchema);

        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
    }

    @Override
    Long read(List<SourceRecord> records, SourceTaskContext context) {
        Long i, offset=ConnectUtils.getStreamOffset(context, partitionField, offsetField, canonicalFilename);
        for (i = 0L; i < batchSize; i++) {

            if (breakAndClose.get())
                break;

            // TODO: filestream may be closed here, fix!

            try {
                Object parsedValue = avroStreamParser.read();

                Map sourcePartition = Collections.singletonMap(partitionField, canonicalFilename);
                Map sourceOffset = Collections.singletonMap(offsetField, offset);

                records.add(new SourceRecord(sourcePartition, sourceOffset, topic, avroStreamParser.connectSchema, parsedValue));
                offset++;
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

    void close() {
        super.close();
        try {
            avroStreamParser.close();
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
    }
}
