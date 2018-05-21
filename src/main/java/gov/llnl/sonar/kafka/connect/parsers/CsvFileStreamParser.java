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
import java.util.Map;
import java.util.NoSuchElementException;

@Slf4j
public class CsvFileStreamParser extends FileStreamParser {

    private CSVFormat csvFormat;
    private CSVParser csvParser;
    private Iterator<CSVRecord> csvRecordIterator;
    private CsvRecordConverter csvRecordConverter;

    private CSVFormat csvFormatFromOptions(Map<String, String> formatOptions) {
        CSVFormat csvFormat = CSVFormat.DEFAULT;
        for (Map.Entry<String, String> option : formatOptions.entrySet()) {
            switch (option.getKey()) {
                case ("withHeader"):
                    Boolean withHeader = Boolean.valueOf(option.getValue());
                    if (withHeader) {
                        csvFormat = csvFormat.withFirstRecordAsHeader();
                    }
                    break;
                case ("columns"):
                    String[] columns = option.getValue().split(",");
                    csvFormat = csvFormat.withHeader(columns);
                    break;
                case ("delimiter"):
                    char delimiter = option.getValue().charAt(0);
                    csvFormat = csvFormat.withDelimiter(delimiter);
                    break;
                case ("quoteChar"):
                    char quoteChar = option.getValue().charAt(0);
                    csvFormat = csvFormat.withQuote(quoteChar);
                    break;
            }
        }
        return csvFormat;
    }

    public CsvFileStreamParser(String filename,
                               Schema avroSchema,
                               Map<String, String> formatOptions) {
        super(filename, avroSchema);

        this.csvFormat = csvFormatFromOptions(formatOptions);
        init();
        csvRecordConverter = new CsvRecordConverter(connectSchema);
    }

    @Override
    void init() {
        try {
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

