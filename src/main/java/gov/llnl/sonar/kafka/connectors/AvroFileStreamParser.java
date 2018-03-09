package gov.llnl.sonar.kafka.connectors;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;

public class AvroFileStreamParser extends ConnectFileStreamParser {

    private Decoder decoder;
    private SpecificDatumReader<GenericData.Record> datumReader;
    private GenericData.Record datum;

    AvroFileStreamParser(FileInputStream in,
                         Schema avroSchema) {

        super(avroSchema);

        this.avroConnectConverter = new AvroData(2);

        try {
            decoder = DecoderFactory.get().jsonDecoder(avroSchema, in);
            datumReader = new SpecificDatumReader<>(avroSchema);
        } catch (Exception ex) {
            log.error(TAG, ex);
        }
    }

    @Override
    public Object read() throws EOFException {

        try {
            datum = datumReader.read(datum, decoder);
        } catch (EOFException e) {
            throw e;
        } catch (IOException e) {
            log.error(TAG + "Error parsing value {}: ", datumReader.getData().toString());
        }

        return avroConnectConverter.toConnectData(connectSchema, datum);

    }
}
