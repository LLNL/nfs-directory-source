package gov.llnl.sonar.kafka.connect.parsers;

import gov.llnl.sonar.kafka.connect.exceptions.ParseException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.connect.errors.DataException;

import java.io.*;

@Slf4j
public class JsonFileStreamParser extends FileStreamParser {

    private Decoder decoder;
    private SpecificDatumReader<GenericData.Record> datumReader;

    public JsonFileStreamParser(String filename,
                                Schema avroSchema,
                                String eofSentinel) {
        super(filename, avroSchema, eofSentinel);

        init();
        datumReader = new SpecificDatumReader<>(avroSchema);
    }

    @Override
    void init() {
    }

    @Override
    public synchronized Object readNextRecord() throws ParseException, EOFException {

        try {
            GenericData.Record datum = new GenericData.Record(avroSchema);
            decoder = DecoderFactory.get().jsonDecoder(avroSchema, nextLine());
            datum = datumReader.read(datum, decoder);
            return datum;
        } catch (AvroTypeException e) {
            log.error("AvroTypeException at {}:{}", filename, e);
            throw new ParseException();
        } catch (DataException e) {
            log.error("DataException at {}:{}", filename, e);
        } catch (EOFException e) {
            throw e;
        } catch (IOException e) {
            log.error("IOException:", e);
            throw new ParseException();
        }

        return null;
    }
}
