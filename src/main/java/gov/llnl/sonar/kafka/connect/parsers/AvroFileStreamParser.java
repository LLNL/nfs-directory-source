package gov.llnl.sonar.kafka.connect.parsers;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.*;

@Slf4j
public class AvroFileStreamParser extends AbstractFileStreamParser {

    private FileInputStream fileStream;

    private Decoder decoder;
    private SpecificDatumReader<GenericData.Record> datumReader;
    private GenericData.Record datum;

    public AvroFileStreamParser(String filename,
                                Schema avroSchema) {

        super(avroSchema);

        try {
            this.fileStream = new FileInputStream(new File(filename));
            decoder = DecoderFactory.get().jsonDecoder(avroSchema, fileStream);
            datumReader = new SpecificDatumReader<>(avroSchema);
        } catch (FileNotFoundException ex) {
            log.error("File {} not found", filename, ex);
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
    }

    @Override
    public Object read() throws EOFException {

        try {
            datum = datumReader.read(datum, decoder);
        } catch (EOFException e) {
            throw e;
        } catch (IOException e) {
            log.error("Error parsing value {}: ", datumReader.getData().toString());
            return null;
        }

        return avroConnectConverter.toConnectData(connectSchema, datum);

    }

    @Override
    public void close() throws IOException {
        fileStream.close();
    }
}
