package gov.llnl.sonar.kafka.connect.parsers;

import gov.llnl.sonar.kafka.connect.converters.CsvRecordConverter;
import gov.llnl.sonar.kafka.connect.exceptions.ParseException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.connect.errors.DataException;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

@Slf4j
public class CsvFileStreamParser extends FileStreamParser {

    //private Map<String, String> csvOptions;
    private CSVFormat csvFormat;
    private CSVParser csvParser;
    private Iterator<CSVRecord> csvRecordIterator;
    private CsvRecordConverter csvRecordConverter;

    //private Boolean withHeader = false;

    public CsvFileStreamParser(String filename,
                               Schema avroSchema) {
                               //Map<String, String> csvOptions) {

        super(filename, avroSchema);

        //this.csvOptions = csvOptions;
        init();
        csvRecordConverter = new CsvRecordConverter(connectSchema);
    }

    @Override
    void init() {
        try {
            csvFormat = CSVFormat.DEFAULT.withFirstRecordAsHeader();

            /*new CSVFormat(
                    delimiter, quoteChar, quoteMode,
                    commentStart, escape, ignoreSurroundingSpaces,
                    ignoreEmptyLines, recordSeparator, nullString,
                    headerComments, header, skipHeaderRecord,
                    allowMissingColumnNames, ignoreHeaderCase, trim,
                    trailingDelimiter);
                    */
            csvParser = csvFormat.parse(inputStreamReader);
            csvRecordIterator = csvParser.iterator();
        } catch (IOException e) {
            log.error("IOException:", e);
        }
    }

    @Override
    public synchronized Object read() throws ParseException, EOFException {

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
            throw new ParseException();
        } catch (NumberFormatException e) {
            log.error("Error parsing {}", csvRecord.toMap());
            log.error("NumberFormatException:", e);
        }

        return null;
    }
}

