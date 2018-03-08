package gov.llnl.sonar.kafka.connectors;

import io.confluent.connect.avro.AvroData;

import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.connect.source.SourceRecord;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

class ConnectFileReader extends ConnectReader {
    private static final Class myClass = ConnectFileReader.class;
    private static final Logger log = LoggerFactory.getLogger(myClass);
    private final String TAG = myClass.getName() + ": ";

    private String canonicalFilename;
    private Path canonicalPath;
    private String topic;

    private org.apache.kafka.connect.data.Schema connectSchema;
    private Long batchSize;
    private String partitionField;
    private String offsetField;

    private InputStream fileStream;
    private JsonDecoder avroJsonDecoder;
    private SpecificDatumReader<GenericData.Record> avroDatumReader;
    private GenericData.Record datum;

    private final AvroData schemaConverter;

    private AtomicBoolean breakAndClose = new AtomicBoolean(false);

    ConnectFileReader(String filename,
                      String topic,
                      org.apache.avro.Schema avroSchema,
                      Long batchSize,
                      String partitionField,
                      String offsetField) {

        this.schemaConverter = new AvroData(2);

        this.topic = topic;
        this.batchSize = batchSize;
        this.partitionField = partitionField;
        this.offsetField = offsetField;

        try {
            File file = new File(filename);
            this.canonicalPath = file.toPath().toRealPath();
            this.canonicalFilename = file.getCanonicalPath();

            fileStream = new FileInputStream(file);
            connectSchema = schemaConverter.toConnectSchema(avroSchema);

            avroJsonDecoder = DecoderFactory.get().jsonDecoder(avroSchema, fileStream);
            avroDatumReader = new SpecificDatumReader<>(avroSchema);
        } catch (Exception ex) {
            log.error(TAG, ex);
        }
    }

    @Override
    Long read(List<SourceRecord> records, SourceTaskContext context) {
        Long i, offset=ConnectUtils.getStreamOffset(context, partitionField, offsetField, canonicalFilename);
        for (i = 0L; i < batchSize; i++) {

            if (breakAndClose.get())
                break;

            try {
                datum = avroDatumReader.read(datum, avroJsonDecoder);
            } catch (EOFException e) {
                try {
                    log.info(TAG + "Purging ingested file {}", canonicalFilename);
                    Files.delete(canonicalPath);
                } catch (IOException e1) {
                    log.error(TAG + "Error deleting file {}", canonicalFilename);
                }
                break;
            } catch (IOException e) {
                log.error(TAG + "Error parsing file {} at row {}: ", canonicalFilename, avroDatumReader.getData().toString());
                continue;
            }

            Map sourcePartition = Collections.singletonMap(partitionField, canonicalFilename);
            Map sourceOffset = Collections.singletonMap(offsetField, offset);

            Object connectValue = schemaConverter.toConnectData(connectSchema, datum);

            records.add(new SourceRecord(sourcePartition, sourceOffset, topic, connectSchema, connectValue));
            offset++;
        }
        return i;
    }

    public String getCanonicalFilename() {
        return canonicalFilename;
    }

    void close() {
        try {
            breakAndClose.set(true);
            fileStream.close();
        } catch (Exception ex) {
            log.error(TAG, ex);
        }
    }
}
