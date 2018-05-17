package gov.llnl.sonar.kafka.connect.readers;

import gov.llnl.sonar.kafka.connect.exceptions.FileLockedException;
import gov.llnl.sonar.kafka.connect.exceptions.FilePurgedException;
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
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

@Slf4j
public class FileReader extends Reader {
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

    public FileReader(String filename,
                      String completedDirectoryName,
                      String topic,
                      org.apache.avro.Schema avroSchema,
                      Long batchSize,
                      String partitionField,
                      String offsetField,
                      String format,
                      FileLock fileLock) {
        this(filename, completedDirectoryName, topic, avroSchema, batchSize, partitionField, offsetField, format);

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
                      String format) {

        this.topic = topic;
        this.batchSize = batchSize;
        this.partitionField = partitionField;
        this.offsetField = offsetField;

        while (!breakAndClose.get()) {
            try {
                File file = new File(filename);
                this.canonicalPath = file.toPath().toRealPath();
                this.canonicalFilename = file.getCanonicalPath();
                this.completedDirectoryPath = Paths.get(completedDirectoryName,filename + ".COMPLETED");
                this.fileChannel = FileChannel.open(canonicalPath, READ, WRITE);

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

            } catch (NoSuchFileException ex) {
                try {
                    synchronized (this) {
                        this.wait(1000);
                    }
                } catch (InterruptedException ex1) {
                    log.error("Who dares to disturb my slumber?");
                }
            } catch (Exception ex) {
                log.error("Exception:", ex);
            }
        }
    }

    private void purgeFile() {
        try {
            log.info("Purging ingested file {}", canonicalFilename);
            Files.move(canonicalPath, completedDirectoryPath);
        } catch (IOException e) {
            log.error("Error moving ingested file {}", canonicalFilename);
            log.error("IOException:", e);
        }
        close();
    }

    private Long safeReturn(Long val, int sleepMillis) {
        try {
            synchronized (this) {
                wait(sleepMillis);
            }
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted");
        }

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
    public Long read(List<SourceRecord> records, SourceTaskContext context)
            throws FileLockedException, FilePurgedException {
        Long i, offset = ConnectUtil.getStreamOffset(context, partitionField, offsetField, canonicalFilename);

        // Try to acquire file lock
        if (fileLock == null) {
            try {
                fileLock = fileChannel.tryLock();
                if (fileLock == null) {
                    log.info("File {} locked, waiting for a second before trying again...", canonicalFilename);
                    return safeReturn(0L, 1000);
                }
            } catch (IOException e) {
                log.error("IOException:", e);
                return safeReturn(0L, 1000);
            }
        }

        // Skip to offset
        try {
            streamParser.skip(offset);
        } catch (EOFException e) {
            purgeFile();
            return safeReturn(0L, 1000);
        }

        // Do the read
        for (i = 0L; i < batchSize; i++) {

            if (breakAndClose.get())
                break;

            try {
                Object parsedValue = streamParser.read();

                if (parsedValue != null) {

                    Map sourcePartition = Collections.singletonMap(partitionField, canonicalFilename);
                    Map sourceOffset = Collections.singletonMap(offsetField, offset);

                    records.add(new SourceRecord(sourcePartition, sourceOffset, topic, streamParser.connectSchema, parsedValue));
                    offset++;

                }

            } catch (EOFException e) {
                purgeFile();
            }

        }
        return safeReturn(i, 1);
    }

    String getCanonicalFilename() {
        return canonicalFilename;
    }

    @Override
    public void close() {
        super.close();
        try {
            if (fileLock != null) {
                fileLock.close();
                fileLock = null;
            }
            streamParser.close();
        } catch (Exception ex) {
            log.error("Exception:", ex);
        }
    }
}
