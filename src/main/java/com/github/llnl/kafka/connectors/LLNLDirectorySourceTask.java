package com.github.llnl.kafka.connectors;

import org.apache.kafka.connect.data.Schema;
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
    private final String TAG = this.getClass().getSimpleName();
    private static final Logger log = LoggerFactory.getLogger(LLNLDirectorySourceTask.class);

    private String filename;
    private BufferedReader reader;
    private String topic;

    private Long streamOffset = 0L;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        log.info(TAG, "start");
        LLNLDirectorySourceConfig config = new LLNLDirectorySourceConfig(map);
        filename = config.getFilename();
        topic = config.getTopic();
        try {
            reader = new BufferedReader(new FileReader(filename));
        } catch (Exception ex) {
            log.error(TAG, ex);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        log.info(TAG, "poll");
        try {
            ArrayList<SourceRecord> records = new ArrayList<>();
            while (records.isEmpty()) {
                String line = reader.readLine();
                if (line != null) {
                    while (line != null) {
                        Map sourcePartition = Collections.singletonMap("filename", filename);
                        Map sourceOffset = Collections.singletonMap("position", streamOffset);
                        records.add(new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, line));
                        streamOffset++;
                        line = reader.readLine();
                    }
                } else {
                    Thread.sleep(1);
                }
            }
            return records;
        } catch (IOException e) {
            // Underlying stream was killed, probably as a result of calling stop. Allow to return
            // null, and driving thread will handle any shutdown if necessary.
        }
        return null;
    }

    @Override
    public void stop() {
        log.info(TAG, "stop");
        try {
            reader.close();
        } catch (Exception ex) {
            log.error(TAG, ex);
        }
    }
}