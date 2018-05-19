package gov.llnl.sonar.kafka.connect.readers;

import gov.llnl.sonar.kafka.connect.exceptions.FileLockedException;
import gov.llnl.sonar.kafka.connect.exceptions.FilePurgedException;
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

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.READ;
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

    public FileReader(String filename,
                      String completedDirectoryName,
                      String topic,
                      org.apache.avro.Schema avroSchema,
                      Long batchSize,
                      String partitionField,
                      String offsetField,
                      String format,
                      Long fileOffset,
                      FileLock fileLock) {
        this(filename, completedDirectoryName, topic, avroSchema, batchSize, partitionField, offsetField, format, fileOffset);

        // Replace filechannel with filelock's channel
        try {
            this.fileChannel.close();
        } catch (IOException e) {
        }
        this.fileChannel = fileLock.channel();
        this.fileLock = fileLock;

    }

    public FileReader(String filename,
                      String completedDirectoryName,
                      String topic,
                      org.apache.avro.Schema avroSchema,
                      Long batchSize,
                      String partitionField,
                      String offsetField,
                      String format,
                      Long fileOffset) {

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
                this.fileChannel = FileChannel.open(completedDirectoryPath, READ, WRITE);

                // Now that we have the lock, make sure the file still exists
                if (!file.exists()) {
                    throw new NoSuchFileException(String.format("File %s does not exist!",filename));
                }

                switch (format) {
                    case "csv":
                        this.streamParser = new CsvFileStreamParser(canonicalFilename, avroSchema);
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
    }

    private void purgeFile() {
        try {
            log.info("Purging ingested file {}", canonicalFilename);
            Files.move(canonicalPath, completedDirectoryPath, REPLACE_EXISTING);
        } catch (IOException e) {
            log.error("Error moving ingested file {}", canonicalFilename);
            log.error("IOException:", e);
        }
        close();
    }

    private Long safeReturn(Long val) {
        if (fileLock != null) {
            try {
                fileLock.close();
                fileLock = null;
            } catch (IOException e) {
                log.error("fileLock.close() IOException:", e);
            }
        }

        return val;
    }

    @Override
    public synchronized Long read(List<SourceRecord> records, SourceTaskContext context)
            throws FileLockedException, FilePurgedException {
        Long i;
        if (currentOffset == 0L) {
            currentOffset = ConnectUtil.getStreamOffset(context, partitionField, offsetField, canonicalFilename);
        }

        // Try to acquire file lock
        if (fileLock == null) {
            try {
                fileLock = fileChannel.tryLock();
                if (fileLock == null) {
                    log.info("File {} locked, waiting for a second before trying again...", canonicalFilename);
                    return safeReturn(0L);
                }
            } catch (OverlappingFileLockException e) {
                return safeReturn(0L);
            } catch (IOException e) {
                log.error("IOException:", e);
                return safeReturn(0L);
            }
        }

        // Skip to offset
        try {
            log.info("Reading from file {} line {}", canonicalFilename, currentOffset);
            streamParser.seekToLine(currentOffset);
        } catch (EOFException e) {
            purgeFile();
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
                purgeFile();
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

    public Long getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public synchronized void close() {
        super.close();
        try {
            if (fileLock != null) {
                fileLock.close();
                fileLock = null;
            }
            fileChannel.close();
            streamParser.close();
        } catch (Exception ex) {
            log.error("Exception:", ex);
        }
    }
}
