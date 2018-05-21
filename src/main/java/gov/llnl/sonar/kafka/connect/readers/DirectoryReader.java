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

import static java.nio.file.StandardOpenOption.*;

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

    private FileReader lockedGetNextFileReader(Iterator<Path> pathWalker) {

        FileReader result = null;
        int numRetries = 100;

        // Get a process-wide lock for this function
        Path pathWalkLockFile = Paths.get(completedDirectoryName, ".directory-walker-lock");
        FileLock pathWalkLock = null;
        while (numRetries >= 0) {
            try {
                pathWalkLock = FileChannel.open(pathWalkLockFile, READ, WRITE, CREATE, SYNC).tryLock();
                break;
            } catch (OverlappingFileLockException e) {
                try {
                    numRetries--;
                    this.wait(100);
                } catch (InterruptedException e1) {
                    return null;
                }
            } catch(IOException e) {
                log.error("IOException:", e);
                return null;
            }
        }

        if (pathWalkLock == null) {
            return null;
        }

        log.info("PathWalker lock acquired");

        result = getNextFileReader(pathWalker);

        try {
            pathWalkLock.release();
            log.info("PathWalker lock released");
        } catch (IOException e) {
            log.error("IOException:", e);
        }

        return result;
    }

    private FileReader getNextFileReader(Iterator<Path> pathWalker) {

        FileLock fileLock = null;
        try {
            while (pathWalker.hasNext()) {
                Path p = pathWalker.next();

                try {
                    // Check the path
                    log.info("Checking path for {}", p.toString());
                    p = p.toRealPath();
                    String pathString = p.toString();

                    // Lock the output location
                    Path completedPath = Paths.get(completedDirectoryName, p.getFileName() + ".COMPLETED");

                    log.info("Getting lock for {}", completedPath.toString());
                    fileLock = FileChannel.open(completedPath, READ, WRITE, CREATE, SYNC).tryLock();
                    log.info("Checking lock for {}", completedPath.toString());

                    if (fileLock != null && fileLock.isValid()) {

                        log.info("Acquired lock for file {}", pathString);

                        // We may have gotten the lock AFTER the file was moved
                        if (Files.notExists(p)) {
                            fileLock.release();
                            log.info("File doesn't exist! Released lock for file {}", pathString);
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
                    log.info("File {} locked, continuing...", p.toString());
                } catch (NoSuchFileException e) {
                    log.info("NoSuchFileException:", e);
                } catch (IOException e) {
                    log.info("Probably a stale file handle, resetting...");
                }
            }
        } catch (UncheckedIOException e) {
            log.info("PathWalker stale, resetting...");
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

                currentFileReader = lockedGetNextFileReader(pathWalker);

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
        notifyAll();
        if (currentFileReader != null) {
            currentFileReader.close();
        }
        log.info("Closed");
    }

}
