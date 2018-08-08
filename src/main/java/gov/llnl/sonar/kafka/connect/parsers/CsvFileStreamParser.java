package gov.llnl.sonar.kafka.connect.parsers;

import gov.llnl.sonar.kafka.connect.exceptions.ParseException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.*;

@Slf4j
public class CsvFileStreamParser extends FileStreamParser {

    private CSVFormat csvFormat;
    private CSVParser csvParser;
    private Iterator<CSVRecord> csvRecordIterator;

    private CSVFormat csvFormatFromOptions(JSONObject formatOptions) {
        CSVFormat csvFormat = CSVFormat.DEFAULT;
        for (String option : formatOptions.keySet()) {
            switch (option) {
                case ("withHeader"):
                    boolean withHeader = formatOptions.getBoolean(option);
                    if (withHeader) {
                        csvFormat = csvFormat.withFirstRecordAsHeader();
                    }
                    break;
                case ("columns"):
                    JSONArray arr = formatOptions.getJSONArray(option);
                    List<String> columns = new ArrayList<>();
                    for(int i = 0; i < arr.length(); i++){
                        columns.add(arr.getString(i));
                    }
                    csvFormat = csvFormat.withHeader(columns.toArray(new String[0]));
                    break;
                case ("delimiter"):
                    char delimiter = formatOptions.getString(option).charAt(0);
                    csvFormat = csvFormat.withDelimiter(delimiter);
                    break;
                case ("quoteChar"):
                    char quoteChar = formatOptions.getString(option).charAt(0);
                    csvFormat = csvFormat.withQuote(quoteChar);
                    break;
                case ("commentChar"):
                    char commentChar = formatOptions.getString(option).charAt(0);
                    csvFormat = csvFormat.withCommentMarker(commentChar);
                    break;
                case ("ignoreSurroundingSpaces"):
                    boolean ignore = formatOptions.getBoolean(option);
                    csvFormat = csvFormat.withIgnoreSurroundingSpaces(ignore);
                    break;
            }
        }
        return csvFormat;
    }

    public CsvFileStreamParser(String filename,
                               Schema avroSchema,
                               String eofSentinel,
                               JSONObject formatOptions) {
        super(filename, avroSchema, eofSentinel);

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

