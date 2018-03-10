package gov.llnl.sonar.kafka.connect.parsers;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.io.*;
import java.util.Iterator;
import java.util.Map;

@Slf4j
public class CsvFileStreamParser extends FileStreamParser {

    private String filename;
    private Reader fileReader;
    private Iterator<CSVRecord> csvRecordIterator;

    private Struct connectRecordBuilder;


    public CsvFileStreamParser(String filename,
                               Schema avroSchema) {

        // TODO: pass in CSV format options here
        super(avroSchema);

        this.filename = filename;

        try {
            fileReader = new FileReader(filename);
            csvRecordIterator = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(fileReader).iterator();
            connectRecordBuilder = new Struct(connectSchema);
        } catch (FileNotFoundException ex) {
            log.error("File {} not found", filename, ex);
        } catch (IOException ex) {
            log.error(ex.getMessage());
        }

    }

    @Override
    public Object read() throws EOFException {

        if (csvRecordIterator.hasNext()) {

            // TODO: how to catch when csv record parse fails?
            log.info("Reading next csv record...");
            CSVRecord csvRecord = csvRecordIterator.next();
            Map<String, String> csvRecordMap = csvRecord.toMap();

            try {
                log.info("Building connect record for csv record " + csvRecord.toString());

                Struct record = connectRecordBuilder;

                // TODO: this will put values as strings, and surely fail...
                for (Map.Entry<String, String> field : csvRecordMap.entrySet()) {
                    record = record.put(field.getKey(), field.getValue());
                }

                return record;

            } catch (DataException ex) {
                log.error("Failed to parse file {}, row {}: {}", filename, csvRecord.toString(), ex.getMessage());
            }
        } else {
            throw new EOFException();
        }

        return null;
    }

    @Override
    public void close() throws IOException {
        fileReader.close();
    }
}

