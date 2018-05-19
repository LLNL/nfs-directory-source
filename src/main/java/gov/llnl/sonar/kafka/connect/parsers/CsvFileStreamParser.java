package gov.llnl.sonar.kafka.connect.parsers;

import gov.llnl.sonar.kafka.connect.converters.CsvRecordConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.connect.errors.DataException;

import java.io.*;
import java.nio.channels.Channels;
import java.util.Iterator;
import java.util.NoSuchElementException;

@Slf4j
public class CsvFileStreamParser extends FileStreamParser {

    private Reader fileReader;
    private CSVParser csvParser;
    private Iterator<CSVRecord> csvRecordIterator;
    private CsvRecordConverter csvRecordConverter;

    public CsvFileStreamParser(String filename,
                               Schema avroSchema) {

        // TODO: pass in CSV format options here
        super(filename, avroSchema);

        csvRecordConverter = new CsvRecordConverter(connectSchema);
    }

    @Override
    void init() {
        try {
            fileReader = new InputStreamReader(Channels.newInputStream(fileChannel));
            csvParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(fileReader);
            csvRecordIterator = csvParser.iterator();
        } catch (IOException e) {
            log.error("IOException:", e);
        }
    }

    @Override
    public synchronized Object read() throws EOFException {

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
    public synchronized void close() throws IOException {
        super.close();
        fileReader.close();
    }
}

