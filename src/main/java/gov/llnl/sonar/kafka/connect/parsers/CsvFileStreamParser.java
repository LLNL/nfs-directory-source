package gov.llnl.sonar.kafka.connect.parsers;

import gov.llnl.sonar.kafka.connect.exceptions.ParseException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.kafka.connect.data.Schema;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.*;

@Slf4j
public class CsvFileStreamParser extends FileStreamParser {

    private CSVFormat csvFormat;
    private Boolean skipHeader = false;
    private String delimString;
    private int numColumns;

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
                               org.apache.avro.Schema avroSchema,
                               String eofSentinel,
                               JSONObject formatOptions) {
        super(filename, avroSchema, eofSentinel);

        this.csvFormat = csvFormatFromOptions(formatOptions);
        init();
    }

    @Override
    void init() {
        try {
            delimString = csvFormat.getDelimiter() + "";
            if (csvFormat.getSkipHeaderRecord()) {
                skipHeader = true;
                String headerLine = nextLine();
                String[] header = headerLine.split(delimString);
                csvFormat = csvFormat.withSkipHeaderRecord(false).withHeader(header);
                numColumns = header.length;
            } else {
                numColumns = -1;
            }
        } catch (IOException e) {
            log.error("IOException:", e);
        }
    }

    @Override
    public synchronized Object readNextRecord() throws ParseException, EOFException {

        String line;
        try {
            if (skipHeader && offset() == 0) {
                nextLine();
            }
            do {
                line = nextLine();
            } while (line.charAt(0) == csvFormat.getCommentMarker());
        } catch (EOFException e) {
            throw e;
        } catch (IOException e) {
            log.error("IOException when reading " + filename + ":" + currentLine + ":", e);
            throw new ParseException();
        }

        try {
            return line.split(delimString, numColumns);
        } catch (NoSuchElementException e) {
            log.error("Empty record at " + filename + ":" + currentLine + "\nContents: + " + line);
            return null;
        }
    }
}

