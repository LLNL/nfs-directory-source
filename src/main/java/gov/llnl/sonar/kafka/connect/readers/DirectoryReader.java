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
import java.util.stream.Stream;

import static java.nio.file.Files.walk;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

@Slf4j
public class DirectoryReader extends Reader {
    private String canonicalDirname;
    private Path dirPath;

    private Long filesPerBatch = 10L;

    private String topic;
    private org.apache.avro.Schema avroSchema;
    private Long batchSize;
    private String partitionField;
    private String offsetField;
    private String format;

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

        this.topic = topic;
        this.avroSchema = avroSchema;
        this.batchSize = batchSize;
        this.partitionField = partitionField;
        this.offsetField = offsetField;
        this.format = format;

        File dir = new File(dirname);
        dirPath = dir.toPath();
        canonicalDirname = dir.getCanonicalPath();

        Boolean isFolder = (Boolean) Files.getAttribute(dir.toPath(), "basic:isDirectory");

        if (!isFolder) {
            throw new IOException(canonicalDirname + " is not a directory!");
        }
    }

    private FileReader getNextFileReader(Path p) {
        try {
            FileLock fileLock = FileChannel.open(p, READ, WRITE).tryLock();
            if (fileLock != null && fileLock.isValid()) {
                 return new FileReader(
                         p.toRealPath().toString(),
                         topic,
                         avroSchema,
                         batchSize,
                         partitionField,
                         offsetField,
                         format,
                         fileLock);
            }
        } catch (OverlappingFileLockException e) {
            log.info("File {} was locked, exception:", e);
        } catch (IOException e) {
            log.error("Exception:", e);
        }
        return null;
    }

    @Override
    public Long read(List<SourceRecord> records, SourceTaskContext context) {

        Long numRecords = 0L;

        try {

            Iterator<Path> pathWalker = Files.walk(dirPath).filter(Files::isRegularFile).iterator();

            while (true) {

                if (breakAndClose.get())
                    throw new BreakException();

                if (!pathWalker.hasNext())
                    throw new BreakException();

                final FileReader reader = getNextFileReader(pathWalker.next());
                try {

                    if (reader == null) {
                        log.error("file reader null due to lock or exception");
                        Thread.sleep(1000);
                        continue;
                    }

                    log.info("Ingesting file {}", reader.getCanonicalFilename());
                    Long numRecordsFile = reader.read(records, context);
                    log.info("Read {} records from file {}", numRecordsFile, reader.getCanonicalFilename());
                    reader.close();

                    numRecords += numRecordsFile;
                } catch (Exception e) {
                    log.error("Exception:", e);
                }
            }
        } catch (IOException e) {
            log.error("Exception:", e);
        } catch (BreakException b) {
            log.info("Reader loop exited");
        }

        return numRecords;
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
