package gov.llnl.sonar.kafka.connect.parsers;

import gov.llnl.sonar.kafka.connect.exceptions.ParseException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.kafka.connect.errors.DataException;
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
                    numColumns = columns.size();
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

    int delimChar;
    StringBuilder sb = new StringBuilder();

    @Override
    void init() {
        try {
            delimString = csvFormat.getDelimiter() + "";
            delimChar = Character.getNumericValue(csvFormat.getDelimiter());

            if (csvFormat.getSkipHeaderRecord()) {
                skipHeader = true;
                ArrayList<String> header = readLineIntoTokens();
                csvFormat = csvFormat.withSkipHeaderRecord(false).withHeader(header.toArray(new String[header.size()]));
                numColumns = header.size();
            } else {
                numColumns = -1;
            }

        } catch (IOException e) {
            log.error("IOException:", e);
        }
    }

    final int newlineChar = Character.getNumericValue('\n');

    private void skipLine() throws IOException {
        int c;
        do {
            c = bufferedReader.read();
            currentByte++;
            if (c == -1) {
                throw new EOFException("End of file reached!");
            }
        } while (c != newlineChar);
        currentLine++;
    }

    private ArrayList<String> readLineIntoTokens() throws IOException {
        ArrayList<String> lineTokens;
        if (numColumns > 0) {
            lineTokens = new ArrayList<>(numColumns);
        } else {
            lineTokens = new ArrayList<>();
        }

        while(true) {
            int c = bufferedReader.read();
            currentByte++;
            if (c == -1) {
                throw new EOFException("EOF sentinel reached!");
            } else if (c == newlineChar) {
                currentLine++;
                break;
            } else if (c == delimChar) {
                lineTokens.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }

        if (numColumns > 0 && lineTokens.size() != numColumns) {
            throw new DataException("Invalid number of columns in " + filename + ":" + String.valueOf(currentLine-1));
        }
        return lineTokens;
    }
    @Override
    public synchronized Object readNextRecord() throws ParseException, EOFException {

        try {
            if (skipHeader && offset() == 0)
                skipLine();
            return readLineIntoTokens();
        } catch (EOFException e) {
            throw e;
        } catch (IOException e) {
            throw new ParseException("IOException when reading " + filename + ":" + String.valueOf(currentLine-1));
        }
    }
}

