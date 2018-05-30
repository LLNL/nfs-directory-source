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
    private String taskid;
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

    FileOffsetManager fileOffsetManager;

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

        this.taskid = InetAddress.getLocalHost().getHostName();
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
            this.fileOffsetManager = new FileOffsetManager("rzsonar8:2181", dirname);
        } catch (Exception e) {
            log.error("Task {}: {}", taskid, e);
        }
    }

    private synchronized <T> T fileOffsetLockedFunction(Callable<T> fn) {

        // Synchronize this block over all threads in the JVM
        synchronized (DirectoryReader.class) {

            // Lock the file offset manager
            if (!fileOffsetManager.lock()) {
                log.info("Task {}: FileOffsetManager failed to acquire lock", taskid);
                return null;
            }

            log.info("Task {}: FileOffsetManager lock acquired", taskid);

            // Download the file offset map
            try {
                fileOffsetManager.download();
            } catch (EOFException e) {
                // empty file offset map, that's ok
            } catch (Exception e) {
                log.error("Task {}: {}", taskid, e);
            }
            log.info("Task {}: Downloaded file offset map {}", taskid, fileOffsetManager.getOffsetMap());

            // Run the function
            T result = null;
            try {
                 result = fn.call();
            } catch (Exception e) {
                log.error("Task {}: {}", taskid, e);
            }

            // Upload the file offset map
            log.info("Task {}: Uploading file offset map {}", taskid, fileOffsetManager.getOffsetMap());
            try {
                fileOffsetManager.upload();
            } catch (Exception e) {
                log.error("Task {}: {}", taskid, e);
            }

            // Unlock the file offset manager
            if (!fileOffsetManager.unlock()) {
                log.info("Task {}: FileOffsetManager failed to release lock", taskid);
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
                                batchSize,
                                partitionField,
                                offsetField,
                                format,
                                formatOptions,
                                offset.offset);
                    } catch (Exception e) {
                        log.info("Task {}: Exception:", taskid, e);
                    }
                }
            }
        } catch (UncheckedIOException e) {
            log.info("Task {}: UncheckedIOException", taskid, e);
        } catch (IOException e) {
            log.info("Task {}: IOException", taskid, e);
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
        Long filesRead = 0L;

        try {

            while (filesRead < filesPerBatch) {

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

                    log.info("Task {}: Ingesting file {}", taskid, currentFileReader.getPath());
                    Long numRecordsFile = currentFileReader.read(records, context);
                    log.info("Task {}: Read {} records from file {}", taskid, numRecordsFile, currentFileReader.getPath());
                    currentFileReader.close();

                    fileOffsetLockedFunction(this::updateFileOffsets);

                    filesRead += 1;
                    numRecords += numRecordsFile;

                } catch (Exception e) {
                    log.error("Task {}: {}", taskid, e);
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
        notifyAll();
        if (currentFileReader != null) {
            currentFileReader.close();
        }
        try {
            fileOffsetManager.close();
        } catch (Exception e) {
            log.error("Task {}: {}", taskid, e);
        }
    }

}
