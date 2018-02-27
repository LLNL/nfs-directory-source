package com.github.llnl.kafka.connectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;

class ConnectDirectoryReader extends ConnectReader {
    private static final Logger log = LoggerFactory.getLogger(ConnectFileReader.class);

    private Long batchSize;
    private String canonicalDirname;
    private Path dirPath;

    private String topic;
    private org.apache.avro.Schema avroSchema;
    private String partitionField;
    private String offsetField;

    private Map<String, ConnectFileReader> fileReaders;

    private String uncheckedGetCanonicalPath(File file) {
        try {
            return file.getCanonicalPath();
        }
        catch(IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    ConnectDirectoryReader(String dirname,
                           String topic,
                           org.apache.avro.Schema avroSchema,
                           Long batchSize,
                           String partitionField,
                           String offsetField)
        throws IOException {

        this.topic = topic;
        this.avroSchema = avroSchema;
        this.batchSize = batchSize;
        this.partitionField = partitionField;
        this.offsetField = offsetField;

        File dir = new File(dirname);
        dirPath = dir.toPath();
        canonicalDirname = dir.getCanonicalPath();

        Boolean isFolder = (Boolean) Files.getAttribute(dir.toPath(),
                "basic:isDirectory", NOFOLLOW_LINKS);

        if (!isFolder) {
            throw new IOException(canonicalDirname + " is not a directory!");
        }

        File[] allFiles = dir.listFiles();

        if (allFiles != null) {
                fileReaders = Arrays.stream(allFiles)
                        .collect(Collectors.toMap(
                                this::uncheckedGetCanonicalPath,
                                (File f) -> new ConnectFileReader(
                                        uncheckedGetCanonicalPath(f),
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

    Path getDirPath() {
        return dirPath;
    }

    String[] getFilenames() {
        return fileReaders.keySet().toArray(new String[0]);
    }

    void addFile(File file) throws IOException {
        String canonicalPath = file.getCanonicalPath();
        fileReaders.put(canonicalPath, new ConnectFileReader(
                canonicalPath,
                topic,
                avroSchema,
                batchSize,
                partitionField,
                offsetField));
    }

    void removeFile(File file) throws IOException {
        fileReaders.remove(file.getCanonicalPath());
    }

    String getCanonicalDirname() {
        return canonicalDirname;
    }

    @Override
    void close() {
        for (ConnectFileReader reader : fileReaders.values()) {
            reader.close();
        }
    }

}
