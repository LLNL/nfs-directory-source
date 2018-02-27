package com.github.llnl.kafka.connectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConnectDirectoryReader extends ConnectReader {
    private static final Logger log = LoggerFactory.getLogger(ConnectFileReader.class);

    private Long batchSize;
    private String absoluteDirname;

    private Map<String, ConnectFileReader> fileReaders;

    ConnectDirectoryReader(String dirname,
                           String topic,
                           org.apache.avro.Schema avroSchema,
                           Long batchSize,
                           String partitionField,
                           String offsetField)
        throws IOException {

        this.batchSize = batchSize;

        File dir = new File(dirname);
        absoluteDirname = dir.getCanonicalPath();

        File[] allFiles = dir.listFiles();
        if (allFiles != null) {
            fileReaders = Arrays.stream(allFiles)
                    .collect(Collectors.toMap(
                            File::getAbsolutePath,
                            (File f) -> new ConnectFileReader(f.getAbsolutePath(),
                                                              topic,
                                                              avroSchema,
                                                              batchSize,
                                                              partitionField,
                                                              offsetField)));
        }
    }

    @Override
    Long read(List<SourceRecord> records, String streamPartition, Long streamOffset) {
        Long i = 0L;
        for (ConnectFileReader reader : fileReaders.values()) {
            i += reader.read(records, reader.getCanonicalFilename(), streamOffset);
            if (i >= batchSize) {
                break;
            }
        }
        return i;
    }

    String getCanonicalDirname() {
        return absoluteDirname;
    }

    @Override
    void close() {
        for (ConnectFileReader reader : fileReaders.values()) {
            reader.close();
        }
    }
}
