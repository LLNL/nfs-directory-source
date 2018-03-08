package com.github.llnl.kafka.connectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

class ConnectDirectoryReader extends ConnectReader {
    private static final Logger log = LoggerFactory.getLogger(ConnectFileReader.class);

    private String canonicalDirname;
    private Path dirPath;

    Supplier<Stream<ConnectFileReader>> fileReaderSupplier;

    private String uncheckedGetCanonicalPath(Path path) {
        try {
            return path.toRealPath().toString();
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

        File dir = new File(dirname);
        dirPath = dir.toPath();
        canonicalDirname = dir.getCanonicalPath();

        Boolean isFolder = (Boolean) Files.getAttribute(dir.toPath(), "basic:isDirectory");

        if (!isFolder) {
            throw new IOException(canonicalDirname + " is not a directory!");
        }

        log.info("Adding all files in {}", canonicalDirname);

        fileReaderSupplier = () -> {
            Stream<ConnectFileReader> fileReaderStream = null;
            try {
                 fileReaderStream = Files.walk(dirPath)
                         .limit(20L)
                         .filter(Files::isRegularFile)
                         .map((Path p) -> new ConnectFileReader(
                                 uncheckedGetCanonicalPath(p),
                                 topic,
                                 avroSchema,
                                 batchSize,
                                 partitionField,
                                 offsetField));
            } catch (IOException e) {
                e.printStackTrace();
            }
            return fileReaderStream;
        };
    }

    @Override
    Long read(List<SourceRecord> records, SourceTaskContext context) {
        return fileReaderSupplier.get()
                .map(reader -> {
                    log.info("Ingesting file {}", reader.getCanonicalFilename());
                    Long numRecords = reader.read(records, context);
                    log.info("Read {} records from file {}", numRecords, reader.getCanonicalFilename());
                    return numRecords;
                })
                .mapToLong(i -> i).sum();
    }

    String getCanonicalDirname() {
        return canonicalDirname;
    }

    @Override
    void close() {
        // do nothing
    }

}
