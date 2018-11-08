package gov.llnl.sonar.kafka.connect.parsers;

import gov.llnl.sonar.kafka.connect.exceptions.ParseException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

import java.io.*;

@Slf4j
public abstract class FileStreamParser {

    private FileReader fileReader = null;
    private BufferedReader bufferedReader = null;
    private int bufferSize = 8192;

    String filename;
    Schema avroSchema;
    String eofSentinel;

    long currentLine = -1l;
    long currentByte = -1l;

    abstract void init();
    public abstract Object readNextRecord() throws ParseException, EOFException;

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

    public synchronized void seekToOffset(Long offset) throws IOException {

        try {
            close();
            fileReader = new FileReader(filename);
            bufferedReader = new BufferedReader(fileReader, bufferSize);
            bufferedReader.skip(offset);
            currentByte = offset;
        } catch (IOException e) {
            log.error("IOException:", e);
        }
    }

    FileStreamParser(String filename, Schema avroSchema, String eofSentinel) {
        this(filename, avroSchema, eofSentinel, 0L);
    }

    FileStreamParser(String filename, Schema avroSchema, String eofSentinel, long startOffset) {
        this.filename = filename;
        this.avroSchema = avroSchema;
        this.eofSentinel = eofSentinel;
    }

    protected String nextLine() throws IOException {

        try {

            if (bufferedReader == null) {
                throw new EOFException("Reader closed!");
            }

            String lineString = bufferedReader.readLine();

            if (lineString == null || (eofSentinel != null && lineString.equals(eofSentinel))) {
                throw new EOFException("EOF sentinel reached!");
            }

            currentLine += 1;
            currentByte += lineString.getBytes().length + 1;

            return lineString;

        } catch (EOFException e) {
            close();
            throw new EOFException("End of file reached!");
        }

    }

    public long offset() {
        return currentByte;
    }
}
