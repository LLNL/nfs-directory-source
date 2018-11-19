package gov.llnl.sonar.kafka.connect.connectors;

import gov.llnl.sonar.kafka.connect.parsers.FileStreamParser;
import gov.llnl.sonar.kafka.connect.parsers.FileStreamParserBuilder;
import gov.llnl.sonar.kafka.connect.offsetmanager.FileOffset;
import gov.llnl.sonar.kafka.connect.offsetmanager.FileOffsetManager;
import gov.llnl.sonar.kafka.connect.util.VersionUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONObject;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
public class DirectorySourceTask extends SourceTask {

    private String taskID;
    private static final String PARTITION_FIELD = "fileName";
    private static final String OFFSET_FIELD = "line";

    DirectorySourceConfig config;
    private FileStreamParserBuilder fileStreamParserBuilder;

    private Path dirPath;
    private Path completedDirPath;

    private FileOffsetManager fileOffsetManager;

    private static final long POLLING_MEMORY_REQUIRED = 8*1000*1000; // 8MB

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {

        config = new DirectorySourceConfig(map);

        try {
            // Get local task id
            this.taskID = InetAddress.getLocalHost().getHostName() + "(" + Thread.currentThread().getId() + ")";

            // Parse avro schema
            org.apache.avro.Schema avroSchema;
            if (!config.getAvroSchema().isEmpty()) {
                avroSchema = new org.apache.avro.Schema.Parser().parse(config.getAvroSchema());
            } else {
                avroSchema = new org.apache.avro.Schema.Parser().parse(new File(config.getAvroSchemaFilename()));
            }

            // Create FileStreamParserBuilder to create FileStreamParsers for files in dir
            fileStreamParserBuilder = new FileStreamParserBuilder();
            fileStreamParserBuilder.setAvroSchema(avroSchema);
            fileStreamParserBuilder.setFormat(config.getFormat());
            fileStreamParserBuilder.setFormatOptions(new JSONObject(config.getFormatOptions()));
            fileStreamParserBuilder.setEofSentinel(config.getEofSentinel());
            fileStreamParserBuilder.setPartitionField(PARTITION_FIELD);
            fileStreamParserBuilder.setOffsetField(OFFSET_FIELD);

            // Set members
            this.dirPath = Paths.get(config.getDirname());
            this.completedDirPath = Paths.get(config.getCompletedDirname());
            this.fileOffsetManager = new FileOffsetManager(
                    config.getZooKeeperHost(),
                    config.getZooKeeperPort(),
                    config.getDirname(),
                    false);

            log.info("Task {}: Added ingestion directory {}", taskID, config.getDirname());

        } catch (Exception ex) {
            log.error("Task {}: {}", taskID, ex);
            this.stop();
        }
    }

    private long approximateAllocatableMemory() {
        return Runtime.getRuntime().maxMemory() -
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
    }

    private List<FileStreamParser> getNextFileStreamParsers() {

        List<FileStreamParser> readers = new ArrayList<>();

        try(Stream<Path> walk = Files.walk(dirPath)) {

            // Walk through all files in dir
            Iterator<Path> pathWalker = walk.filter(Files::isRegularFile).iterator();

            fileOffsetManager.lock();

            while (readers.size() < config.getBatchFiles() && pathWalker.hasNext()) {

                // Next file in dir
                Path p = pathWalker.next().toAbsolutePath();

                // Get and lock file getByteOffset if it is available
                final FileOffset fileOffset = fileOffsetManager.downloadFileOffsetWithLock(p.toString());

                // If not locked or completed, lock it and create a reader
                if (fileOffset != null) {
                    try {
                        readers.add(fileStreamParserBuilder.build(p));
                        log.debug("Task {}: Created file reader for {}: {}", taskID, p.toString(), fileOffset);
                    } catch (Exception e) {
                        log.error("Task {}: {}", taskID, e);
                    }
                }
            }
        } catch (IOException | UncheckedIOException e) {
            // Don't care about NoSuchFileException, that's just NFS catching up
            if (!(ExceptionUtils.getRootCause(e) instanceof NoSuchFileException)) {
                log.error("Task {}: {}", taskID, e);
            }
        } catch (Exception e) {
            log.error("Task {}: {}", taskID, e);
        }

        fileOffsetManager.unlock();

        return readers;
    }

    @Override
    public synchronized List<SourceRecord> poll() throws InterruptedException {

        Long mem;
        if ((mem = approximateAllocatableMemory()) < POLLING_MEMORY_REQUIRED) {
            log.warn("Task {}: Available memory {} less than required amount {}", taskID, mem, POLLING_MEMORY_REQUIRED);
            log.warn("Task {}: Polling paused for 1 second and sending hint to garbage collect", taskID);
            System.gc(); // tell the system to garbage collect soon
            this.wait(1000);
            return null;
        }

        List<SourceRecord> records = new ArrayList<>();
        List<FileStreamParser> currentFileStreamParsers = getNextFileStreamParsers();

        try {

            // Read from each FileStreamParser
            for (FileStreamParser currentFileStreamParser : currentFileStreamParsers) {

                // Read batches of rows
                int rows = 0;
                try {
                    for (rows = 0; rows < config.getBatchRows(); rows++) {
                        records.add(currentFileStreamParser.readNextRecord(config.getTopic()));
                    }
                } catch (EOFException e) {
                    log.info("Task {}: Reached end of file {}", taskID, currentFileStreamParser.getFileName());
                    if (config.getDeleteIngested()) {
                        currentFileStreamParser.deleteFile();
                    } else if (config.getCompletedDirname() != null) {
                        currentFileStreamParser.moveFileIntoDirectory(dirPath, completedDirPath);
                    }
                }

                if (rows > 0) {
                    log.info("Task {}: Read {} records from file {}", taskID, rows, currentFileStreamParser.getFileName());
                }

                currentFileStreamParser.close();
            }
        } catch (Exception e) {
            log.error("Task {}: ", taskID, e);
            synchronized (this) {
                this.wait(1000);
            }
        }

        // If empty, return null
        if (records.isEmpty()) {
            records = null;
            synchronized (this) {
                this.wait(1000);
            }
        } else {
            log.info("Task {}: Read {} records from directory {}", taskID, records.size(), config.getDirname());
        }

        return records;
    }

    @Override
    public synchronized void stop() {
        try {
            fileOffsetManager.close();
            synchronized (this) {
                this.notify();
            }
            // TODO: close current FileStreamParser(?)
        } catch (Exception e) {
            log.error("Task {}: {}", taskID, e);
        }
    }
}

