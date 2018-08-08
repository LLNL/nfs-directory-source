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
    private GenericData.Record datum;

    public JsonFileStreamParser(String filename,
                                Schema avroSchema,
                                String eofSentinel) {
        super(filename, avroSchema, eofSentinel);

        init();
        datumReader = new SpecificDatumReader<>(avroSchema);
    }

    @Override
    void init() {
        try {
            decoder = DecoderFactory.get().jsonDecoder(avroSchema, fileInputStream);
        } catch (FileNotFoundException e) {
            log.error("FileNotFoundException:", e);
        } catch (IOException e) {
            log.error("IOException:", e);
        } catch (Exception e) {
            log.error("Exception:", e);
        }

    }

    @Override
    public synchronized Object read() throws ParseException, EOFException {

        try {
            datum = datumReader.read(datum, decoder);
            currentLine++;
            return datum;
        } catch (AvroTypeException e) {
            log.error("AvroTypeException at {}:{}", filename, currentLine, e);
            throw new ParseException();
        } catch (DataException e) {
            log.error("DataException at {}:{}", filename, currentLine, e);
        } catch (EOFException e) {
            throw e;
        } catch (IOException e) {
            log.error("IOException:", e);
            throw new ParseException();
        }

        return null;
    }
}
