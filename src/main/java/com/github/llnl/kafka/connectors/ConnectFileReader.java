package com.github.llnl.kafka.connectors;

import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ConnectFileReader {
    private static final Logger log = LoggerFactory.getLogger(ConnectFileReader.class);

    private String filename;
    private String topic;

    private org.apache.kafka.connect.data.Schema connectSchema;
    private Long batchSize;

    private InputStream fileStream;
    private JsonDecoder avroJsonDecoder;
    private SpecificDatumReader<GenericData.Record> avroDatumReader;
    private GenericData.Record datum;

    ConnectFileReader(String filename, String topic, org.apache.avro.Schema avroSchema, Long batchSize) {
        this.filename = filename;
        this.topic = topic;
        this.batchSize = batchSize;

        try {
            fileStream = new FileInputStream(filename);
            connectSchema = SchemaUtils.avroToKafkaConnectSchema(avroSchema);

            avroJsonDecoder = DecoderFactory.get().jsonDecoder(avroSchema, fileStream);
            avroDatumReader = new SpecificDatumReader<>(avroSchema);

            log.debug("TASK avro schema: " + avroSchema);
            log.debug("TASK kafka connect schema: " + connectSchema);
            for (Field field : connectSchema.fields()) {
                log.debug(String.format("TASK kafka connect field: %s %s", field.name(),field.schema().type()));
            }

        } catch (Exception ex) {
            log.error("TASK ", ex);
        }
    }

    Long read(List<SourceRecord> records, Long streamOffset) {
        Long i, offset=streamOffset;
        for (i = 0L; i < batchSize; i++) {

            try {
                datum = avroDatumReader.read(datum, avroJsonDecoder);
            } catch (EOFException e) {
                break;
            } catch (IOException e) {
                log.error("Error parsing data: " + avroDatumReader.getSpecificData());
                continue;
            }

            Map sourcePartition = Collections.singletonMap("filename", filename);
            Map sourceOffset = Collections.singletonMap("position", offset);

            Struct struct = SchemaUtils.genericDataRecordToKafkaConnectStruct(datum, connectSchema);

            records.add(new SourceRecord(sourcePartition, sourceOffset, topic, connectSchema, struct));
            offset++;
        }
        return i;
    }

    public String getFilename() {
        return filename;
    }

    void close() {
        try {
            fileStream.close();
        } catch (Exception ex) {
            log.error("TASK ", ex);
        }
    }
}
