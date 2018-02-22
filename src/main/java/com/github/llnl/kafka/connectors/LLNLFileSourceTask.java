package com.github.llnl.kafka.connectors;

import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class LLNLFileSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(LLNLFileSourceTask.class);

    private String filename;
    private String topic;

    private InputStream fileStream;
    private JsonDecoder avroJsonDecoder;
    private SpecificDatumReader<GenericData.Record> avroDatumReader;
    private GenericData.Record datum;

    private org.apache.avro.Schema avroSchema;
    private org.apache.kafka.connect.data.Schema connectSchema;

    private Long streamOffset = 0L;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        LLNLFileSourceConfig config = new LLNLFileSourceConfig(map);
        filename = config.getFilename();
        topic = config.getTopic();
        try {
            fileStream = new FileInputStream(filename);

            avroSchema = new org.apache.avro.Schema.Parser().parse(config.getAvroSchema());
            avroJsonDecoder = DecoderFactory.get().jsonDecoder(avroSchema, fileStream);
            avroDatumReader = new SpecificDatumReader<>(avroSchema);

            connectSchema = SchemaUtils.avroToKafkaConnectSchema(avroSchema);

            // Debug printing
            log.debug("TASK avro schema: " + avroSchema);
            log.debug("TASK kafka connect schema: " + connectSchema);
            for (Field field : connectSchema.fields()) {
                log.debug(String.format("TASK kafka connect field: %s %s", field.name(),field.schema().type()));
            }

        } catch (Exception ex) {
            log.error("TASK " + ex);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try {
            ArrayList<SourceRecord> records = new ArrayList<>();
            while (records.isEmpty()) {
                try {
                    while (true) {
                        datum = avroDatumReader.read(datum, avroJsonDecoder);

                        Map sourcePartition = Collections.singletonMap("filename", filename);
                        Map sourceOffset = Collections.singletonMap("position", streamOffset);

                        Struct struct = SchemaUtils.genericDataRecordToKafkaConnectStruct(datum, connectSchema);

                        records.add(new SourceRecord(sourcePartition, sourceOffset, topic, connectSchema, struct));
                        streamOffset++;
                    }
                } catch (EOFException e) {
                    Thread.sleep(1);
                } catch (IOException e) {
                    log.error("Error parsing data: " + avroDatumReader.getSpecificData());
                }
            }
            return records;
        } catch (Exception e) {
            log.error("Exception: " + e.getMessage());
        }
        return null;
    }

    @Override
    public void stop() {
        log.info("TASK stop");
        try {
            fileStream.close();
        } catch (Exception ex) {
            log.error("TASK " + ex);
        }
    }
}