package gov.llnl.sonar.kafka.connect.readers;

import gov.llnl.sonar.kafka.connect.exceptions.BreakException;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.nio.file.Files.walk;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

@Slf4j
public class DirectoryReader extends Reader {
    private String canonicalDirname;
    private String completedDirectoryName;
    private Path dirPath;

    private Long filesPerBatch = 10L;

    private String topic;
    private org.apache.avro.Schema avroSchema;
    private Long batchSize;
    private String partitionField;
    private String offsetField;
    private String format;
    Map<String, String> formatOptions;

    private FileReader currentFileReader;

    Map<String, Long> currentOffsets = new HashMap<String, Long>();

    public DirectoryReader(String dirname,
                           String completedDirectoryName,
                           String topic,
                           org.apache.avro.Schema avroSchema,
                           Long batchSize,
                           String partitionField,
                           String offsetField,
                           String format,
                           Map<String, String> formatOptions)
        throws IOException {

        this.completedDirectoryName = completedDirectoryName;
        this.topic = topic;
        this.avroSchema = avroSchema;
        this.batchSize = batchSize;
        this.partitionField = partitionField;
        this.offsetField = offsetField;
        this.format = format;
        this.formatOptions = formatOptions;

        File dir = new File(dirname);
        dirPath = dir.toPath();
        canonicalDirname = dir.getCanonicalPath();

        if (Files.notExists(dirPath)) {
            throw new FileNotFoundException();
        }

        Boolean isFolder = (Boolean) Files.getAttribute(dirPath, "basic:isDirectory");

        if (!isFolder) {
            throw new IOException(canonicalDirname + " is not a directory!");
        }
    }

    private FileReader getNextFileReader(Iterator<Path> pathWalker) {

        while(pathWalker.hasNext()) {
            Path p = pathWalker.next();
            try {
                // Check the path
                log.info("Checking path for {}", p.toString());
                p = p.toRealPath();
                String pathString = p.toString();

                // Lock the output location
                Path completedPath = Paths.get(completedDirectoryName,p.getFileName() + ".COMPLETED");

                log.info("Getting lock for {}", completedPath.toString());
                FileLock fileLock = FileChannel.open(completedPath, READ, WRITE, CREATE).tryLock();
                log.info("Checking lock for {}", completedPath.toString());

                if (fileLock != null && fileLock.isValid()) {

                    log.info("Lock acquired for {}", pathString);

                    // We may have gotten the lock AFTER the file was moved
                    if (Files.notExists(p)) {
                        fileLock.close();
                        throw new NoSuchFileException(String.format("File %s does not exist!", pathString));
                    }

                    // Check if we have an offset stored
                    Long fileOffset = 0L;
                    if (currentOffsets.containsKey(pathString)) {
                        fileOffset = currentOffsets.get(pathString);
                    }

                    // Lock acquired and file exists!
                    return new FileReader(
                            pathString,
                            completedDirectoryName,
                            topic,
                            avroSchema,
                            batchSize,
                            partitionField,
                            offsetField,
                            format,
                            formatOptions,
                            fileOffset,
                            fileLock);
                }
            } catch (OverlappingFileLockException e) {
                log.info("OverlappingFileLockException:", e);
            } catch (NoSuchFileException e) {
                log.info("NoSuchFileException:", e);
            } catch (IOException e) {
                log.error("IOException:", e);
            }
        }
        return null;
    }

    @Override
    public synchronized Long read(List<SourceRecord> records, SourceTaskContext context) {

        Long numRecords = 0L;
        Long filesRead = 0L;

        try {

            Iterator<Path> pathWalker = Files.walk(dirPath).filter(Files::isRegularFile).iterator();

            while (filesRead < filesPerBatch) {

                if (breakAndClose.get()) {
                    log.info("Reader interrupted, exiting reader loop");
                    throw new BreakException();
                }

                currentFileReader = getNextFileReader(pathWalker);

                if (currentFileReader == null) {
                    log.info("All files processed, exiting currentFileReader loop");
                    throw new BreakException();
                }

                try {

                    log.info("Ingesting file {}", currentFileReader.getCanonicalFilename());
                    Long numRecordsFile = currentFileReader.read(records, context);
                    log.info("Read {} records from file {}", numRecordsFile, currentFileReader.getCanonicalFilename());
                    currentFileReader.close();

                    currentOffsets.put(currentFileReader.getFilename(), currentFileReader.getCurrentOffset());

                    filesRead += 1;
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
    public synchronized void close() {
        log.info("Interrupting reader");
        breakAndClose.set(true);
        if (currentFileReader != null) {
            currentFileReader.close();
        }
        log.info("Closed");
    }

}
