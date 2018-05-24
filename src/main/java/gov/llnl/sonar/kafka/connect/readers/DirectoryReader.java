package gov.llnl.sonar.kafka.connect.readers;

import gov.llnl.sonar.kafka.connect.connectors.DirectorySourceConnector;
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
import java.util.concurrent.Callable;

import static java.nio.file.StandardOpenOption.*;

@Slf4j
public class DirectoryReader extends Reader {
    private String canonicalDirname;
    private String completedDirectoryName;
    private Path dirPath;

    private Long filesPerBatch = 1L;

    private String topic;
    private org.apache.avro.Schema avroSchema;
    private Long batchSize;
    private String partitionField;
    private String offsetField;
    private String format;
    Map<String, Object> formatOptions;

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
                           Map<String, Object> formatOptions)
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

    private <T> T superLock(Callable<T> fn) {

        synchronized (DirectoryReader.class) {

            int numRetries = 100;

            // Get a process-wide lock for this function
            Path pathWalkLockFile = Paths.get(completedDirectoryName, DirectorySourceConnector.LOCK_FILENAME);
            FileLock pathWalkLock = null;
            while (numRetries >= 0) {
                try {
                    pathWalkLock = FileChannel.open(pathWalkLockFile, READ, WRITE, SYNC).tryLock();
                    break;
                } catch (OverlappingFileLockException e) {
                    try {
                        numRetries--;
                        this.wait(100);
                    } catch (InterruptedException e1) {
                        return null;
                    }
                } catch (IOException e) {
                    log.error("IOException:", e);
                    return null;
                }
            }

            if (pathWalkLock == null) {
                return null;
            }

            log.debug("PathWalker lock acquired");

            T result = null;
            try {
                 result = fn.call();
            } catch (Exception e) {
                log.error("Exception:", e);
            }

            try {
                pathWalkLock.release();
                log.debug("PathWalker lock released");
            } catch (IOException e) {
                log.error("IOException:", e);
            }

            return result;
        }
    }

    private FileReader getNextFileReader() {

        Iterator<Path> pathWalker;
        FileLock fileLock;
        try {
            pathWalker = Files.walk(dirPath).filter(Files::isRegularFile).iterator();

            while (pathWalker.hasNext()) {
                Path p = pathWalker.next();

                try {
                    // Check the path
                    log.debug("Checking path for {}", p.toString());
                    p = p.toRealPath();
                    String pathString = p.toString();

                    // Lock the output location
                    Path completedPath = Paths.get(completedDirectoryName, p.getFileName() + ".COMPLETED");

                    log.debug("Getting lock for {}", completedPath.toString());
                    fileLock = FileChannel.open(completedPath, READ, WRITE, CREATE, SYNC).tryLock();
                    log.debug("Checking lock for {}", completedPath.toString());

                    if (fileLock != null && fileLock.isValid()) {

                        log.debug("Acquired lock for file {}", pathString);

                        // We may have gotten the lock AFTER the file was moved
                        if (Files.notExists(p)) {
                            fileLock.release();
                            log.debug("File doesn't exist! Released lock for file {}", pathString);
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
                                fileLock.channel(),
                                fileLock);
                    }
                } catch (OverlappingFileLockException e) {
                    log.debug("File {} locked, continuing...", p.toString());
                } catch (NoSuchFileException e) {
                    log.debug("NoSuchFileException:", e);
                } catch (IOException e) {
                    log.debug("Probably a stale file handle, resetting...");
                }
            }
        } catch (UncheckedIOException e) {
            log.debug("PathWalker stale, resetting...");
        } catch (IOException e) {
            // nothing important
        }
        return null;
    }

    private int purgeCurrentFileReader() {
        currentFileReader.purgeFile();
        return 0;
    }

    @Override
    public Long read(List<SourceRecord> records, SourceTaskContext context) {

        Long numRecords = 0L;
        Long filesRead = 0L;

        try {

            while (filesRead < filesPerBatch) {

                if (breakAndClose.get()) {
                    log.debug("Reader interrupted, exiting reader loop");
                    throw new BreakException();
                }

                currentFileReader = superLock(this::getNextFileReader);

                if (currentFileReader == null) {
                    log.debug("All files either locked or processed, exiting currentFileReader loop");
                    throw new BreakException();
                }

                try {

                    log.debug("Ingesting file {}", currentFileReader.getCanonicalFilename());
                    Long numRecordsFile = currentFileReader.read(records, context);
                    log.info("Read {} records from file {}", numRecordsFile, currentFileReader.getCanonicalFilename());
                    currentFileReader.close();

                    currentOffsets.put(currentFileReader.getFilename(), currentFileReader.getCurrentOffset());

                    if (currentFileReader.ingestCompleted) {
                        superLock(this::purgeCurrentFileReader);
                    }

                    filesRead += 1;
                    numRecords += numRecordsFile;

                } catch (Exception e) {
                    log.error("Exception:", e);
                }
            }
        } catch (BreakException b) {
            log.debug("Reader loop exited");
        }

        return numRecords;
    }

    public String getCanonicalDirname() {
        return canonicalDirname;
    }

    @Override
    public synchronized void close() {
        log.debug("Interrupting reader");
        breakAndClose.set(true);
        notifyAll();
        if (currentFileReader != null) {
            currentFileReader.close();
        }
        log.debug("Closed");
    }

}
