package gov.llnl.sonar.kafka.connect.readers;

import gov.llnl.sonar.kafka.connect.exceptions.FileLockedException;
import gov.llnl.sonar.kafka.connect.exceptions.ParseException;
import gov.llnl.sonar.kafka.connect.parsers.CsvFileStreamParser;
import gov.llnl.sonar.kafka.connect.parsers.FileStreamParser;
import gov.llnl.sonar.kafka.connect.parsers.JsonFileStreamParser;
import gov.llnl.sonar.kafka.connect.util.ConnectUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.WRITE;

@Slf4j
public class FileReader extends Reader {
    private String filename;
    private String canonicalFilename;
    private Path canonicalPath;
    private Path completedDirectoryPath;
    private String topic;
    private FileChannel fileChannel;
    private FileLock fileLock;

    private Long batchSize;
    private String partitionField;
    private String offsetField;

    private FileStreamParser streamParser;

    private Long currentOffset;

    public Boolean ingestCompleted = false;

    public FileReader(String filename,
                      String completedDirectoryName,
                      String topic,
                      org.apache.avro.Schema avroSchema,
                      Long batchSize,
                      String partitionField,
                      String offsetField,
                      String format,
                      Map<String, Object> formatOptions,
                      Long fileOffset,
                      FileChannel fileChannel,
                      FileLock fileLock) {

        this.filename = filename;
        this.topic = topic;
        this.batchSize = batchSize;
        this.partitionField = partitionField;
        this.offsetField = offsetField;
        this.currentOffset = fileOffset;

        while (!breakAndClose.get()) {
            try {
                File file = new File(filename);

                this.canonicalPath = file.toPath().toRealPath();
                this.canonicalFilename = file.getCanonicalPath();

                // TODO: handle name collisions /dir/foo/file1 /dir/bar/file1
                this.completedDirectoryPath = Paths.get(completedDirectoryName,canonicalPath.getFileName() + ".COMPLETED");

                // Get file channel if not exists
                if (fileChannel == null) {
                    this.fileChannel = FileChannel.open(completedDirectoryPath, READ, WRITE, SYNC);
                } else {
                    this.fileChannel = fileChannel;
                }

                // Get file lock if not exists
                if (fileLock == null) {
                    this.fileLock = this.fileChannel.tryLock();
                } else {
                    this.fileLock = fileLock;
                }

                // Now that we have the lock, make sure the file still exists
                if (!file.exists()) {
                    throw new NoSuchFileException(String.format("File %s does not exist!",filename));
                }

                switch (format) {
                    case "csv":
                        this.streamParser = new CsvFileStreamParser(canonicalFilename, avroSchema, formatOptions);
                        break;
                    case "json":
                        this.streamParser = new JsonFileStreamParser(canonicalFilename, avroSchema);
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid file format " + format);
                }

                break;

            } catch (NoSuchFileException e) {
                log.error("NoSuchFileException:", e);
            } catch (Exception e) {
                log.error("Exception:", e);
            }
        }

        log.info("Added ingestion file {}", canonicalFilename);
    }

    public void purgeFile() {
        try {
            log.info("Purging ingested file {}", canonicalFilename);
            Files.move(canonicalPath, completedDirectoryPath, ATOMIC_MOVE, REPLACE_EXISTING);
        } catch (NoSuchFileException e) {
            log.debug("File {} already purged");
        } catch(IOException e) {
            log.error("Error moving ingested file {}", canonicalFilename);
            log.error("IOException:", e);
        }
        close();
    }

    private Long safeReturn(Long val) {
        if (fileLock != null) {
            try {
                fileLock.release();
                fileLock = null;
                log.debug("Released lock for file {}", canonicalFilename);
            } catch (IOException e) {
                log.error("IOException:", e);
            }
        }

        return val;
    }

    @Override
    public Long read(List<SourceRecord> records, SourceTaskContext context)
            throws FileLockedException {

        // TODO: filechannel can get closed after this ?!
        if (ingestCompleted || !fileChannel.isOpen()) {
            return 0L;
        }

        Long i;
        if (currentOffset == 0L) {
            currentOffset = ConnectUtil.getStreamOffset(context, partitionField, offsetField, canonicalFilename);
        }

        // Try to acquire file lock
        if (fileLock == null) {
            try {
                fileLock = fileChannel.tryLock();
                if (fileLock == null) {
                    log.debug("File {} locked, waiting for a second before trying again...", canonicalFilename);
                    return safeReturn(0L);
                }
            } catch (OverlappingFileLockException e) {
                return safeReturn(0L);
            } catch (IOException e) {
                log.error("IOException:", e);
                return safeReturn(0L);
            }
        }

        log.debug("Acquired lock for file {}", canonicalFilename);

        // Skip to offset
        try {
            log.info("Reading from file {} line {}", canonicalFilename, currentOffset);
            streamParser.seekToLine(currentOffset);
        } catch (EOFException | FileNotFoundException e) {
            ingestCompleted = true;
            close();
            return safeReturn(0L);
        }

        // Do the read
        for (i = 0L; i < batchSize; i++) {

            if (breakAndClose.get())
                break;

            try {
                Object parsedValue = streamParser.read();

                if (parsedValue != null) {

                    Map sourcePartition = Collections.singletonMap(partitionField, canonicalFilename);
                    Map sourceOffset = Collections.singletonMap(offsetField, currentOffset);

                    records.add(new SourceRecord(sourcePartition, sourceOffset, topic, streamParser.connectSchema, parsedValue));

                    currentOffset++;
                }

            } catch (ParseException e) {
                log.error("Record parse failed, closing reader");
                close();
            } catch (EOFException e) {
                ingestCompleted = true;
                close();
            } catch (Exception e) {
                log.error("Exception:", e);
            }

        }
        return safeReturn(i);
    }

    String getFilename() {
        return filename;
    }

    String getCanonicalFilename() {
        return canonicalFilename;
    }

    Long getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public synchronized void close() {
        super.close();
        try {
            if (fileLock != null) {
                fileLock.release();
                fileLock = null;
                log.debug("Released lock for file {}", canonicalFilename);
            }
            fileChannel.close();
            streamParser.close();
        } catch (Exception ex) {
            log.error("Exception:", ex);
        }
    }
}
