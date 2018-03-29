package gov.llnl.sonar.kafka.connect.parsers;

import gov.llnl.sonar.kafka.connect.converters.CsvRecordConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

@Slf4j
public class CsvFileStreamParser extends FileStreamParser {

    private String filename;
    private Reader fileReader;
    private Iterator<CSVRecord> csvRecordIterator;

    private CsvRecordConverter csvRecordConverter;

    public CsvFileStreamParser(String filename,
                               Schema avroSchema) {

        // TODO: pass in CSV format options here
        super(avroSchema);

        this.filename = filename;

        try {
            fileReader = new FileReader(filename);
            csvRecordIterator = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(fileReader).iterator();
            csvRecordConverter = new CsvRecordConverter(connectSchema);
        } catch (FileNotFoundException ex) {
            log.error("File {} not found", filename, ex);
        } catch (IOException ex) {
            log.error("Exception:", ex);
        }

    }

    @Override
    public Object read() throws EOFException {

        // TODO: how to catch when csv record parse fails?

        try {
            log.debug("Reading next csv record...");
            CSVRecord csvRecord = csvRecordIterator.next();

            try {
                log.debug("Building connect record for csv record {}", csvRecord.toString());
                return csvRecordConverter.convert(csvRecord);

            } catch (DataException ex) {
                log.error("Failed to convert csv record {}, from file {}", csvRecord.toString(), filename, ex);
            }
        } catch (NoSuchElementException e) {
            throw new EOFException();
        }

        return null;
    }

    @Override
    public void skip(Long numRecords) throws EOFException {
        for(Long i = 0L; i < numRecords; i++) {
            try {
                csvRecordIterator.next();
            } catch (NoSuchElementException e) {
                throw new EOFException();
            }
        }
    }

    @Override
    public void close() throws IOException {
        fileReader.close();
    }
}

