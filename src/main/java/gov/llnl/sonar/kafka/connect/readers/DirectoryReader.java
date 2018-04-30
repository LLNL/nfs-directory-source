package gov.llnl.sonar.kafka.connect.readers;

import gov.llnl.sonar.kafka.connect.exceptions.BreakException;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Supplier;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

@Slf4j
public class DirectoryReader extends Reader {
    private String canonicalDirname;
    private Path dirPath;

    private Long filesPerBatch = 10L;

    private Supplier<Iterator<FileReader>> fileReaderSupplier;

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
            Iterator<FileReader> fileReaderStream = null;
            try {
                fileReaderStream = Files.walk(dirPath)
                        .filter(Files::isRegularFile)
                        .map((Path p) -> {
                            try {
                                FileLock fileLock = FileChannel.open(p, READ, WRITE).tryLock();
                                if (fileLock != null && fileLock.isValid())
                                    return new HashMap.SimpleImmutableEntry<FileLock, Path>(fileLock, p);
                            } catch (OverlappingFileLockException | IOException e) {
                            }
                            return null;
                        })
                        .filter(Objects::nonNull)
                        .filter((HashMap.SimpleImmutableEntry<FileLock, Path> lockedPath) -> lockedPath.getKey().isValid())
                        .limit(filesPerBatch)
                        .map((HashMap.SimpleImmutableEntry<FileLock, Path> lockedPath) -> new FileReader(
                                uncheckedGetCanonicalPath(lockedPath.getValue()),
                                topic,
                                avroSchema,
                                batchSize,
                                partitionField,
                                offsetField,
                                format,
                                lockedPath.getKey()))
                        .iterator();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return fileReaderStream;
        };
    }

    @Override
    public Long read(List<SourceRecord> records, SourceTaskContext context) {

        try {

            final Iterator<FileReader> fileReaders = fileReaderSupplier.get();
            Long numRecords = 0L;

            while (true) {

                if (breakAndClose.get())
                    throw new BreakException();

                final FileReader reader;
                try {
                    if (fileReaders.hasNext())
                        reader = fileReaders.next();
                    else
                        break;
                } catch (UncheckedIOException ioe) {
                    continue;
                }

                log.info("Ingesting file {}", reader.getCanonicalFilename());
                Long numRecordsFile = reader.read(records, context);
                log.info("Read {} records from file {}", numRecordsFile, reader.getCanonicalFilename());
                reader.close();

                numRecords += numRecordsFile;
            }

            return numRecords;

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
