package gov.llnl.sonar.kafka.connect.parsers;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.*;
import java.nio.channels.Channels;

@Slf4j
public class JsonFileStreamParser extends FileStreamParser {

    private InputStream inputStream;
    private Decoder decoder;
    private SpecificDatumReader<GenericData.Record> datumReader;
    private GenericData.Record datum;

    public JsonFileStreamParser(String filename,
                                Schema avroSchema) {
        super(filename, avroSchema);

        datumReader = new SpecificDatumReader<>(avroSchema);
    }

    @Override
    void init() {
        try {
            inputStream = Channels.newInputStream(fileChannel);
            decoder = DecoderFactory.get().jsonDecoder(avroSchema, inputStream);
        } catch (FileNotFoundException e) {
            log.error("FileNotFoundException:", e);
        } catch (IOException e) {
            log.error("IOException:", e);
        } catch (Exception e) {
            log.error("Exception:", e);
        }

    }

    @Override
    public synchronized Object read() throws EOFException {

        try {
            datum = datumReader.read(datum, decoder);
        } catch (EOFException e) {
            throw e;
        } catch (IOException e) {
            log.error("Error parsing value {}", datumReader.getData().toString());
            log.error("IOException:", e);
            return null;
        }

        return avroConnectConverter.toConnectData(connectSchema, datum);

    }

    @Override
    public synchronized void close() throws IOException {
        super.close();
        inputStream.close();
    }
}
