package gov.llnl.sonar.kafka.connect.parsers;

import gov.llnl.sonar.kafka.connect.converters.CsvRecordConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
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
        } catch (IOException e) {
            log.error("IOException:", e);
        }

    }

    @Override
    public Object read() throws EOFException {

        final CSVRecord csvRecord;
        try {
            csvRecord = csvRecordIterator.next();
        } catch (NoSuchElementException e) {
            throw new EOFException();
        }

        try {
            return csvRecordConverter.convert(csvRecord.toMap());
        } catch (DataException e) {
            log.error("Error parsing {}", csvRecord.toMap());
            log.error("DataException:", e);
        } catch (NumberFormatException e) {
            log.error("Error parsing {}", csvRecord.toMap());
            log.error("NumberFormatException:", e);
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

