package gov.llnl.sonar.kafka.connectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

class ConnectDirectoryReader extends ConnectReader {
    private String canonicalDirname;
    private Path dirPath;

    private Long filesPerBatch = 10L;

    private Supplier<Stream<ConnectFileReader>> fileReaderSupplier;

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

        log.info(TAG + "Adding all files in {}", canonicalDirname);

        fileReaderSupplier = () -> {
            Stream<ConnectFileReader> fileReaderStream = null;
            try {
                 fileReaderStream = Files.walk(dirPath)
                         .filter(Files::isRegularFile)
                         .limit(filesPerBatch)
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

        try {

            final Stream<ConnectFileReader> fileReaders = fileReaderSupplier.get();

            return fileReaders.map(reader -> {

                if (breakAndClose.get())
                    throw new BreakException();

                log.info(TAG + "Ingesting file {}", reader.getCanonicalFilename());
                Long numRecordsFile = reader.read(records, context);
                log.info(TAG + "Read {} records from file {}", numRecordsFile, reader.getCanonicalFilename());
                reader.close();

                return numRecordsFile;
            }).mapToLong(l -> l).sum();

        } catch (BreakException b) {
            log.info(TAG + "Read interrupted, closing reader");
        }

        return -1L;
    }

    String getCanonicalDirname() {
        return canonicalDirname;
    }

    @Override
    void close() {
        log.info(TAG + "Interrupting reader");
        breakAndClose.set(true);
        log.info(TAG + "Closed");
    }

}
