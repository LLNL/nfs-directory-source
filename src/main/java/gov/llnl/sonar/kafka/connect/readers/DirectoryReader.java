package gov.llnl.sonar.kafka.connect.readers;

import gov.llnl.sonar.kafka.connect.exceptions.BreakException;
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
public class DirectoryReader extends AbstractReader {
    private String canonicalDirname;
    private Path dirPath;

    private Long filesPerBatch = 10L;

    private Supplier<Stream<FileReader>> fileReaderSupplier;

    private String uncheckedGetCanonicalPath(Path path) {
        try {
            return path.toRealPath().toString();
        }
        catch(IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public DirectoryReader(String dirname,
                           String topic,
                           org.apache.avro.Schema avroSchema,
                           Long batchSize,
                           String partitionField,
                           String offsetField,
                           String format)
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
            Stream<FileReader> fileReaderStream = null;
            try {
                 fileReaderStream = Files.walk(dirPath)
                         .filter(Files::isRegularFile)
                         .limit(filesPerBatch)
                         .map((Path p) -> new FileReader(
                                 uncheckedGetCanonicalPath(p),
                                 topic,
                                 avroSchema,
                                 batchSize,
                                 partitionField,
                                 offsetField,
                                 format));
            } catch (IOException e) {
                e.printStackTrace();
            }
            return fileReaderStream;
        };
    }

    @Override
    public Long read(List<SourceRecord> records, SourceTaskContext context) {

        try {

            final Stream<FileReader> fileReaders = fileReaderSupplier.get();

            // TODO: sleep if no files in directory

            return fileReaders.map(reader -> {

                if (breakAndClose.get())
                    throw new BreakException();

                log.info("Ingesting file {}", reader.getCanonicalFilename());
                Long numRecordsFile = reader.read(records, context);
                log.info("Read {} records from file {}", numRecordsFile, reader.getCanonicalFilename());
                reader.close();

                return numRecordsFile;
            }).mapToLong(l -> l).sum();

        } catch (BreakException b) {
            log.info("Read interrupted, closing reader");
        }

        return -1L;
    }

    public String getCanonicalDirname() {
        return canonicalDirname;
    }

    @Override
    public void close() {
        log.info("Interrupting reader");
        breakAndClose.set(true);
        log.info("Closed");
    }

}
