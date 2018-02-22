package com.github.llnl.kafka.connectors;

import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
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
    private BufferedReader reader;

    private org.apache.avro.Schema avro_schema;
    private Schema connect_schema;

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
            reader = new BufferedReader(new FileReader(filename));

            avro_schema = new org.apache.avro.Schema.Parser().parse(config.getAvroSchema());
            log.info("TASK avro schema: " + avro_schema);

            connect_schema = SchemaUtils.avroToKafkaSchema(avro_schema);
            log.info("TASK kafka connect schema: " + connect_schema);
            for (Field field : connect_schema.fields()) {
                log.info("TASK kafka connect field name: " + field.name());
                log.info("TASK kafka connect field type: " + field.schema().type());
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
                // TODO: use jsonDecoder(avro_schema, inputStream) instead of FileInputStream
                String line = reader.readLine();
                if (line != null) {
                    while (line != null) {
                        Map sourcePartition = Collections.singletonMap("filename", filename);
                        Map sourceOffset = Collections.singletonMap("position", streamOffset);

                        log.info("TASK line: " + line);
                        log.info("TASK creating json decoder...");

                        JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(avro_schema, line);

                        log.info("TASK creating datum reader...");
                        SpecificDatumReader<GenericData.Record> datumReader = new SpecificDatumReader<>(avro_schema);

                        log.info("TASK reading datum...");
                        GenericData.Record datum = datumReader.read(null, jsonDecoder);

                        log.info("TASK datum: " + datum);

                        // TODO: make function to convert GenericData.Record to a connect Struct (under a connect schema)
                        Struct struct = new Struct(connect_schema);
                        for (Field field : connect_schema.fields()) {
                            String name = field.name();
                            Object value = datum.get(field.name());

                            if (value instanceof org.apache.avro.util.Utf8) {
                                value = value.toString();
                            }

                            log.info("TASK adding field: " + name);
                            log.info("TASK adding value: " + value);
                            struct = struct.put(field.name(), value);
                        }

                        records.add(new SourceRecord(sourcePartition, sourceOffset, topic, connect_schema, struct));
                        streamOffset++;

                        line = reader.readLine();
                    }
                } else {
                    Thread.sleep(1);
                }
            }
            return records;
        } catch (Exception e) {
            // Underlying stream was killed, probably as a result of calling stop. Allow to return
            // null, and driving thread will handle any shutdown if necessary.
            log.error("Exception: " + e.getMessage());
        }
        return null;
    }

    @Override
    public void stop() {
        log.info("TASK stop");
        try {
            reader.close();
        } catch (Exception ex) {
            log.error("TASK " + ex);
        }
    }
}