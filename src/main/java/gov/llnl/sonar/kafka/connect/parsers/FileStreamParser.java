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
    AvroData avroConnectConverter;
    Schema avroSchema;
    public org.apache.kafka.connect.data.Schema connectSchema;
    Long currentLine = 0L;

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

    FileStreamParser(String filename, Schema avroSchema) {
        this.avroSchema = avroSchema;
        this.avroConnectConverter = new AvroData(2);

        try {
            // NOTE: Subclasses must use EITHER fileInputStream OR dataInputStream.
            //       Using both causes undefined behavior!
            this.filename = filename;
            seekToLine(0L, false);
            connectSchema = avroConnectConverter.toConnectSchema(avroSchema);
        } catch (IOException e) {
            log.error("IOException:", e);
        } catch (Exception e) {
            log.error("Exception:", e);
        }
    }

}
