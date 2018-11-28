package gov.llnl.sonar.kafka.connect.parsers;

import io.confluent.connect.avro.AvroData;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

@Slf4j
public abstract class FileStreamParser {

    // File semantics
    String fileName;
    Path filePath;
    JSONObject formatOptions;
    String eofSentinel;

    // Schema semantics
    AvroData avroData;
    org.apache.avro.Schema avroSchema;
    org.apache.kafka.connect.data.Schema connectSchema;

    // File reading vars
    int bufferSize;
    long byteOffset;
    FileReader fileReader;
    BufferedReader bufferedReader;
    final int newLineChar = (int) '\n';

    // Kafka vars
    String partitionField;
    String offsetField;
    Map<String, String> sourcePartition;

    // Abstract functions
    public abstract SourceRecord readNextRecord(String topic) throws IOException;

    public FileStreamParser(Path filePath,
                            JSONObject formatOptions,
                            AvroData avroData,
                            Schema avroSchema,
                            org.apache.kafka.connect.data.Schema connectSchema,
                            String eofSentinel,
                            int bufferSize,
                            long byteOffset,
                            String partitionField,
                            String offsetField) throws IOException {
        this.filePath = filePath;
        this.fileName = filePath.toString();
        this.formatOptions = formatOptions;
        this.avroData = avroData;
        this.avroSchema = avroSchema;
        this.connectSchema = connectSchema;
        this.eofSentinel = eofSentinel;
        this.bufferSize = bufferSize;
        this.byteOffset = byteOffset;
        this.partitionField = partitionField;
        this.offsetField = offsetField;

        this.sourcePartition = Collections.singletonMap(partitionField, fileName);

        seekToOffset(byteOffset);
    }

    public String getFileName() {
        return fileName;
    }

    public Path getFilePath() {
        return filePath;
    }

    public Map<String, Long> getSourceOffset() {
        return Collections.singletonMap(offsetField, byteOffset);
    }

    public synchronized void deleteFile() throws IOException {
        Files.delete(filePath);
    }

    /**
     * Moves the current file found within sourceDir to destDir with subdirectories.
     * If file is /a/b/c.txt, sourceDir is /a and destDir is /x, will create
     * subdirectory /x/b and move file to /x/b/c.txt
     *
     * @param sourceDir
     * @param destDir
     * @throws IOException
     */
    public synchronized void moveFileIntoDirectory(Path sourceDir, Path destDir) throws IOException {
        Path relativePathToFile = sourceDir.relativize(filePath).normalize();
        Path completedFilePath = destDir.resolve(relativePathToFile).normalize();
        Path completedFileParentPath = completedFilePath.getParent();
        Files.createDirectories(completedFileParentPath);
        Files.move(filePath, completedFilePath);
    }

    public long getByteOffset() {
        return byteOffset;
    }

    public synchronized void seekToOffset(Long offset) throws IOException {
        close();
        fileReader = new FileReader(fileName);
        bufferedReader = new BufferedReader(fileReader, bufferSize);
        bufferedReader.skip(offset);
        byteOffset = offset;
    }

    synchronized void skipLine() throws IOException {
        int c;
        do {
            c = bufferedReader.read();
            byteOffset++;
            if (c == -1) {
                throw new EOFException("End of file reached!");
            }
        } while (c != newLineChar);
    }

    synchronized String nextLine() throws IOException {
        try {

            if (bufferedReader == null) {
                throw new EOFException("Reader closed!");
            }

            String lineString = bufferedReader.readLine();
            byteOffset += lineString.getBytes().length + 1;

            if (lineString == null || (eofSentinel != null && lineString.equals(eofSentinel))) {
                throw new EOFException("EOF sentinel reached!");
            }

            return lineString;

        } catch (EOFException e) {
            close();
            throw new EOFException("End of file reached!");
        }
    }

    public void close() throws IOException {
        if (bufferedReader != null) {
            bufferedReader.close();
            bufferedReader = null;
        }
        if (fileReader != null) {
            fileReader.close();
            fileReader = null;
        }
    }
}
