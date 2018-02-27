package com.github.llnl.kafka.connectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LLNLDirectorySourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(LLNLDirectorySourceTask.class);
    private String TAG = getClass().getName() + ": ";
    private static final String PARTITION_FIELD = "dirname";
    private static final String OFFSET_FIELD = "position";

    private Long streamOffset = 0L;
    private String canonicalDirname;

    private ConnectDirectoryReader reader;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        LLNLDirectorySourceConfig config = new LLNLDirectorySourceConfig(map);
        try {
            String relativeDirname = config.getDirname();

            org.apache.avro.Schema avroSchema;
            if (!config.getAvroSchema().isEmpty()) {
                avroSchema = new org.apache.avro.Schema.Parser().parse(config.getAvroSchema());
            } else {
                avroSchema = new org.apache.avro.Schema.Parser().parse(new File(config.getAvroSchemaFilename()));
            }

            reader = new ConnectDirectoryReader(relativeDirname,
                                                config.getTopic(),
                                                avroSchema,
                                                config.getBatchSize(),
                                                PARTITION_FIELD,
                                                OFFSET_FIELD);

            canonicalDirname = reader.getCanonicalDirname();

            // TODO: create a directory watch callback here

        } catch (Exception ex) {
            log.error(TAG, ex);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // TODO: if directory watch found something, reconfigure reader here

        ArrayList<SourceRecord> records = new ArrayList<>();
        streamOffset = ConnectUtils.getStreamOffset(context, PARTITION_FIELD, OFFSET_FIELD, canonicalDirname);

        try {
            streamOffset += reader.read(records, null, streamOffset);
            return records;
        } catch (Exception e) {
            log.error(TAG, e);
        }
        return null;
    }

    @Override
    public void stop() {
        log.info(TAG + "stop");
        try {
            reader.close();
        } catch (Exception ex) {
            log.error(TAG, ex);
        }
    }
}

