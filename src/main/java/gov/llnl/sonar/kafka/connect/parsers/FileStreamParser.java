package gov.llnl.sonar.kafka.connect.parsers;

import gov.llnl.sonar.kafka.connect.exceptions.ParseException;
import io.confluent.connect.avro.AvroData;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

import java.io.*;

@Slf4j
public abstract class FileStreamParser {

    String filename;
    FileInputStream fileInputStream = null;
    DataInputStream dataInputStream = null;
    InputStreamReader inputStreamReader = null;
    Schema avroSchema;
    Long currentLine = 0L;
    String eofSentinel;

    abstract void init();
    public abstract Object read() throws ParseException, EOFException;

    public void close() throws IOException {
        if (inputStreamReader != null) {
            inputStreamReader.close();
            inputStreamReader = null;
        }
        if (dataInputStream != null) {
            dataInputStream.close();
            dataInputStream = null;
        }
        if (fileInputStream != null) {
            fileInputStream.close();
            fileInputStream = null;
        }
    }

    public synchronized void seekToLine(Long line) throws IOException {
        seekToLine(line, true);
    }

    public synchronized void seekToLine(Long line, Boolean init) throws IOException {
        close();
        fileInputStream = new FileInputStream(new File(filename));
        dataInputStream = new DataInputStream(fileInputStream);
        for (Long l=0L; l<line; l++) {
            dataInputStream.readLine();
        }
        inputStreamReader = new InputStreamReader(dataInputStream);
        currentLine = line;
        if (init) {
            init();
        }
    }

    FileStreamParser(String filename, Schema avroSchema, String eofSentinel) {
        this.avroSchema = avroSchema;
        this.eofSentinel = eofSentinel;

        try {
            // NOTE: Subclasses must use EITHER fileInputStream OR dataInputStream.
            //       Using both causes undefined behavior!
            this.filename = filename;
            seekToLine(0L, false);
        } catch (IOException e) {
            log.error("IOException:", e);
        } catch (Exception e) {
            log.error("Exception:", e);
        }
    }

}
