package gov.llnl.sonar.kafka.connect.readers;

import gov.llnl.sonar.kafka.connect.exceptions.FileLockedException;
import gov.llnl.sonar.kafka.connect.exceptions.ParseException;
import gov.llnl.sonar.kafka.connect.parsers.CsvFileStreamParser;
import gov.llnl.sonar.kafka.connect.parsers.FileStreamParser;
import gov.llnl.sonar.kafka.connect.parsers.JsonFileStreamParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

@Slf4j
public class FileReader extends Reader {
    private String taskID;
    private Path path;
    private Path completedFilePath;
    private String topic;

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
                      Long fileOffset) throws UnknownHostException {
        this(new File(filename).toPath().toAbsolutePath(),
                completedDirectoryName, topic, avroSchema, batchSize,
                partitionField, offsetField, format, formatOptions, fileOffset);
    }

    public FileReader(Path path,
                      String completedDirectoryName,
                      String topic,
                      org.apache.avro.Schema avroSchema,
                      Long batchSize,
                      String partitionField,
                      String offsetField,
                      String format,
                      Map<String, Object> formatOptions,
                      Long fileOffset)
            throws UnknownHostException {

        this.taskID = InetAddress.getLocalHost().getHostName() + "(" + Thread.currentThread().getId() + ")";
        this.path = path;
        this.topic = topic;
        this.batchSize = batchSize;
        this.partitionField = partitionField;
        this.offsetField = offsetField;
        this.currentOffset = fileOffset;

        while (!breakAndClose.get()) {
            try {
                // TODO: handle name collisions /dir/foo/file1 /dir/bar/file1
                this.completedFilePath = Paths.get(completedDirectoryName,path.getFileName() + ".COMPLETED");

                if (Files.notExists(path)) {
                    throw new NoSuchFileException(String.format("File %s does not exist!", path));
                }

                switch (format) {
                    case "csv":
                        this.streamParser = new CsvFileStreamParser(path.toString(), avroSchema, formatOptions);
                        break;
                    case "json":
                        this.streamParser = new JsonFileStreamParser(path.toString(), avroSchema);
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid file format " + format);
                }

                break;

            } catch (NoSuchFileException e) {
                log.error("Task {}: NoSuchFileException:", taskID, e);
            } catch (Exception e) {
                log.error("Task {}: Exception:", taskID, e);
            }
        }

        log.debug("Task {}: Added ingestion file {}", taskID, path);
    }

    public void purgeFile() {
        try {
            log.debug("Task {}: Purging ingested file {}", taskID, path);
            Files.move(path, completedFilePath);
        } catch(IOException e) {
            log.error("Task {}: Error moving ingested file {}", taskID, path);
            log.error("Task {}: {}", taskID, e);
        }
        close();
    }

    @Override
    public synchronized Long read(List<SourceRecord> records, SourceTaskContext context)
            throws FileLockedException {

        if (ingestCompleted) {
            return 0L;
        }

        // Skip to offset
        try {
            log.debug("Task {}: Reading from file {} line {}", taskID, path, currentOffset);
            streamParser.seekToLine(currentOffset);
        } catch (EOFException | FileNotFoundException e) {
            ingestCompleted = true;
            currentOffset = -1L;
            log.debug("Task {}: Ingest from file {} complete!", taskID, path);
            close();
            return 0L;
        } catch (IOException e) {
            log.error("Task {}: {}", taskID, e);
        }

        // Do the read
        Long i;
        for (i = 0L; i < batchSize; i++) {

            if (breakAndClose.get())
                break;

            try {
                Object parsedValue = streamParser.read();

                if (parsedValue != null) {

                    Map sourcePartition = Collections.singletonMap(partitionField, path.toString());
                    Map sourceOffset = Collections.singletonMap(offsetField, currentOffset);

                    records.add(new SourceRecord(sourcePartition, sourceOffset, topic, streamParser.connectSchema, parsedValue));

                    currentOffset++;
                }

            } catch (ParseException e) {
                log.error("Record parse failed, closing reader");
                close();
            } catch (EOFException e) {
                ingestCompleted = true;
                currentOffset = -1L;
                log.debug("Task {}: Ingest from file {} complete!", taskID, path);
                close();
            } catch (Exception e) {
                log.error("Task {}: {}", taskID, e);
            }

        }

        log.debug("Task {}: Read {} records from file {}", taskID, i, path);

        return i;
    }

    Path getPath() {
        return path;
    }

    Long getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public synchronized void close() {
        super.close();
        try {
            streamParser.close();
        } catch (Exception ex) {
            log.error("Task {}: {}", taskID, ex);
        }
    }
}
