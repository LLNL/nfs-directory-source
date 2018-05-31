package gov.llnl.sonar.kafka.connect.readers;

import gov.llnl.sonar.kafka.connect.exceptions.BreakException;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.io.*;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;


@Slf4j
public class DirectoryReader extends Reader {
    private String taskID;
    private String canonicalDirname;
    private String completedDirectoryName;
    private Path dirPath;

    private String topic;
    private org.apache.avro.Schema avroSchema;
    private Long batchSize;
    private Long remainingBatch;
    private String partitionField;
    private String offsetField;
    private String format;
    private Map<String, Object> formatOptions;

    private FileReader currentFileReader;

    private FileOffsetManager fileOffsetManager;

    public DirectoryReader(String dirname,
                           String completedDirectoryName,
                           String topic,
                           org.apache.avro.Schema avroSchema,
                           Long batchSize,
                           String partitionField,
                           String offsetField,
                           String format,
                           Map<String, Object> formatOptions,
                           String zooKeeperHost,
                           String zooKeeperPort)
            throws IOException {

        this.taskID = InetAddress.getLocalHost().getHostName() + "(" + Thread.currentThread().getId() + ")";
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

        try {
            this.fileOffsetManager = new FileOffsetManager(zooKeeperHost, zooKeeperPort, dirname);
        } catch (Exception e) {
            log.error("Task {}: {}", taskID, e);
        }
    }

    private synchronized <T> T fileOffsetLockedFunction(Callable<T> fn) {

        // Synchronize this block over all threads in the JVM
        synchronized (DirectoryReader.class) {

            // Lock the file offset manager
            if (!fileOffsetManager.lock()) {
                log.debug("Task {}: FileOffsetManager failed to acquire lock", taskID);
                return null;
            }

            log.debug("Task {}: FileOffsetManager lock acquired", taskID);

            // Download the file offset map
            try {
                fileOffsetManager.download();
            } catch (Exception e) {
                log.error("Task {}: {}", taskID, e);
            }
            log.debug("Task {}: Downloaded file offset map {}", taskID, fileOffsetManager.getOffsetMap());

            // Run the function
            T result = null;
            try {
                 result = fn.call();
            } catch (Exception e) {
                log.error("Task {}: {}", taskID, e);
            }

            // Upload the file offset map
            log.debug("Task {}: Uploading file offset map {}", taskID, fileOffsetManager.getOffsetMap());
            try {
                fileOffsetManager.upload();
            } catch (Exception e) {
                log.error("Task {}: {}", taskID, e);
            }

            // Unlock the file offset manager
            if (!fileOffsetManager.unlock()) {
                log.debug("Task {}: FileOffsetManager failed to release lock", taskID);
            }

            return result;
        }
    }

    /** MUST BE CALLED IN fileOffsetLockedFunction */
    private synchronized FileReader getNextFileReader() {

        try {

            Iterator<Path> pathWalker = Files.walk(dirPath).filter(Files::isRegularFile).iterator();
            HashMap<String, FileOffset> fileOffsetMap = fileOffsetManager.getOffsetMap();

            while (pathWalker.hasNext()) {

                Path p = pathWalker.next().toAbsolutePath();

                // Get offset from file offset map if it exists
                final FileOffset offset;
                if (fileOffsetMap.containsKey(p.toString())) {
                    offset = fileOffsetMap.get(p.toString());
                } else {
                    offset = new FileOffset(0L, false, false);
                }

                // If not locked or completed, lock it and create a reader
                if (!offset.locked && !offset.completed) {

                    // Lock file and publish offset to map
                    offset.setLocked(true);
                    fileOffsetMap.put(p.toString(), offset);
                    fileOffsetManager.setOffsetMap(fileOffsetMap);

                    try {
                        return new FileReader(
                                p,
                                completedDirectoryName,
                                topic,
                                avroSchema,
                                remainingBatch,
                                partitionField,
                                offsetField,
                                format,
                                formatOptions,
                                offset.offset);
                    } catch (Exception e) {
                        log.error("Task {}: {}", taskID, e);
                    }
                }
            }
        } catch (IOException | UncheckedIOException e) {
            log.error("Task {}: {}", taskID, e);
        }
        return null;
    }

    /** MUST BE CALLED IN fileOffsetLockedFunction */
    private synchronized int updateFileOffsets() {

        HashMap<String, FileOffset> fileOffsetMap = fileOffsetManager.getOffsetMap();

        // Unlock file offset and publish
        fileOffsetMap.put(currentFileReader.getPath().toString(),
                new FileOffset(currentFileReader.getCurrentOffset(), false, currentFileReader.ingestCompleted));

        // Purge the file if completed
        if (currentFileReader.ingestCompleted) {
            currentFileReader.purgeFile();
            currentFileReader = null;
        }

        fileOffsetManager.setOffsetMap(fileOffsetMap);

        return 0;
    }

    @Override
    public synchronized Long read(List<SourceRecord> records, SourceTaskContext context) {

        Long numRecords = 0L;

        remainingBatch = batchSize;

        try {

            while (remainingBatch > 0L) {

                if (breakAndClose.get()) {
                    log.debug("Reader interrupted, exiting reader loop");
                    throw new BreakException();
                }

                currentFileReader = fileOffsetLockedFunction(this::getNextFileReader);

                if (currentFileReader == null) {
                    log.debug("All files either locked or processed, exiting currentFileReader loop");
                    throw new BreakException();
                }

                try {

                    Long numRecordsFile = currentFileReader.read(records, context);
                    currentFileReader.close();

                    fileOffsetLockedFunction(this::updateFileOffsets);

                    remainingBatch -= numRecordsFile;
                    numRecords += numRecordsFile;

                } catch (Exception e) {
                    log.error("Task {}: {}", taskID, e);
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
        breakAndClose.set(true);
        if (currentFileReader != null) {
            currentFileReader.close();
        }
        try {
            fileOffsetManager.close();
        } catch (Exception e) {
            log.error("Task {}: {}", taskID, e);
        }
    }

}
