package com.github.llnl.kafka.connectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConnectDirectoryReader extends ConnectReader {
    private static final Logger log = LoggerFactory.getLogger(ConnectFileReader.class);

    private Long batchSize;

    private Map<String, ConnectFileReader> fileReaders;

    ConnectDirectoryReader(String dirname, String topic, org.apache.avro.Schema avroSchema, Long batchSize)
        throws FileNotFoundException {

        this.batchSize = batchSize;

        File[] allFiles = new File(dirname).listFiles();
        if (allFiles != null) {
            fileReaders = Arrays.stream(allFiles)
                    .collect(Collectors.toMap(
                            File::getAbsolutePath,
                            (File f) -> new ConnectFileReader(f.getAbsolutePath(), topic, avroSchema, batchSize)));
        }
    }

    @Override
    Long read(List<SourceRecord> records, Long streamOffset) {
        Long i = 0L;
        for (ConnectFileReader reader : fileReaders.values()) {
            i += reader.read(records, streamOffset);
            if (i >= batchSize) {
                break;
            }
        }
        return i;
    }

    @Override
    void close() {
        for (ConnectFileReader reader : fileReaders.values()) {
            reader.close();
        }
    }
}
