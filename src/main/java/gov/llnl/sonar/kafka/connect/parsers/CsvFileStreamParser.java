package gov.llnl.sonar.kafka.connect.parsers;

import gov.llnl.sonar.kafka.connect.converters.CsvRecordConverter;
import gov.llnl.sonar.kafka.connect.exceptions.ParseException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.*;

@Slf4j
public class CsvFileStreamParser extends FileStreamParser {

    private CSVFormat csvFormat;
    private CSVParser csvParser;
    private Iterator<CSVRecord> csvRecordIterator;
    private CsvRecordConverter csvRecordConverter;

    private CSVFormat csvFormatFromOptions(Map<String, Object> formatOptions) {
        CSVFormat csvFormat = CSVFormat.DEFAULT;
        for (Map.Entry<String, Object> option : formatOptions.entrySet()) {
            switch (option.getKey()) {
                case ("withHeader"):
                    Boolean withHeader = (Boolean) option.getValue();
                    if (withHeader) {
                        csvFormat = csvFormat.withFirstRecordAsHeader();
                    }
                    break;
                case ("columns"):
                    List<String> columns = (List<String>) option.getValue();
                    csvFormat = csvFormat.withHeader(columns.toArray(new String[0]));
                    break;
                case ("delimiter"):
                    char delimiter = ((String) option.getValue()).charAt(0);
                    csvFormat = csvFormat.withDelimiter(delimiter);
                    break;
                case ("quoteChar"):
                    char quoteChar = ((String) option.getValue()).charAt(0);
                    csvFormat = csvFormat.withQuote(quoteChar);
                    break;
            }
        }
        return csvFormat;
    }

    public CsvFileStreamParser(String filename,
                               Schema avroSchema,
                               Map<String, Object> formatOptions) {
        super(filename, avroSchema);

        this.csvFormat = csvFormatFromOptions(formatOptions);
        init();
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
        try {
            return csvRecordIterator.next().toMap();
        } catch (NoSuchElementException e) {
            throw new EOFException();
        }
    }
}

