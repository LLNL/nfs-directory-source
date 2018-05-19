package gov.llnl.sonar.kafka.connect.parsers;

import io.confluent.connect.avro.AvroData;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import static java.nio.file.StandardOpenOption.READ;

@Slf4j
public abstract class FileStreamParser {

    FileChannel fileChannel;
    AvroData avroConnectConverter;
    Schema avroSchema;
    public org.apache.kafka.connect.data.Schema connectSchema;

    abstract void init();
    public abstract Object read() throws EOFException;

    public void close() throws IOException {
        fileChannel.close();
    }

    public Long position() throws IOException {
       return fileChannel.position();
    }

    public synchronized void seek(Long position) throws EOFException {
        try {
            fileChannel = fileChannel.position(position);
            init();
        } catch (EOFException e) {
            throw e;
        } catch (IOException e) {
            log.error("IOException:", e);
        }
    }

    FileStreamParser(String filename, Schema avroSchema) {

        this.avroSchema = avroSchema;
        this.avroConnectConverter = new AvroData(2);

        try {
            this.fileChannel = FileChannel.open(Paths.get(filename), READ);
            connectSchema = avroConnectConverter.toConnectSchema(avroSchema);
            init();
        } catch (IOException e) {
            log.error("IOException:", e);
        } catch (Exception e) {
            log.error("Exception:", e);
        }
    }

}
