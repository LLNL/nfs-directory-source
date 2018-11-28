package gov.llnl.sonar.kafka.connect.parsers;

import gov.llnl.sonar.kafka.connect.converters.CsvRecordConverter;
import io.confluent.connect.avro.AvroData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

@Slf4j
public class CsvFileStreamParser extends FileStreamParser {

    // CSV semantics
    private int numColumns;
    private String[] columns = null;
    private int delimChar = (int)',';
    private int commentChar = (int)'#';
    private boolean hasHeader = true;

    // Vars for parsing
    private StringBuilder sb = new StringBuilder();
    private CsvRecordConverter csvRecordConverter;

    public CsvFileStreamParser(Path filePath,
                               JSONObject formatOptions,
                               AvroData avroData,
                               org.apache.avro.Schema avroSchema,
                               org.apache.kafka.connect.data.Schema connectSchema,
                               String eofSentinel,
                               int bufferSize,
                               long byteOffset,
                               String partitionField,
                               String offsetField) throws IOException {
        super(filePath,
              formatOptions,
              avroData,
              avroSchema,
              connectSchema,
              eofSentinel,
              bufferSize,
              byteOffset,
              partitionField,
              offsetField);

        parseCsvFormatOptions(formatOptions);
        parseHeader();

        csvRecordConverter = new CsvRecordConverter(connectSchema, columns);
    }

    private synchronized void parseHeader() throws IOException {
        if (columns == null && hasHeader) {
            columns = readCsvLineIntoTokens();
            numColumns = columns.length;
        } else if (columns == null) {
            throw new IllegalArgumentException("No header and no columns specified for CSV parser!");
        }
    }

    private void parseCsvFormatOptions(JSONObject formatOptions) {
        for (String option : formatOptions.keySet()) {
            switch (option) {
                case ("withHeader"):
                    hasHeader = formatOptions.getBoolean(option);
                    break;
                case ("columns"):
                    JSONArray arr = formatOptions.getJSONArray(option);
                    columns = new String[arr.length()];
                    for(int i = 0; i < arr.length(); i++){
                        columns[i] = arr.getString(i);
                    }
                    numColumns = columns.length;
                    break;
                case ("delimiter"):
                    delimChar = formatOptions.getString(option).charAt(0);
                    break;
                case ("commentChar"):
                    commentChar = formatOptions.getString(option).charAt(0);
                    break;
            }
        }
    }

    private synchronized String[] readCsvLineIntoTokens() throws IOException {

        if (bufferedReader == null) {
            throw new EOFException("EOF reached!");
        }

        // Container for tokens to return
        ArrayList<String> lineTokens;
        if (numColumns > 0) {
            lineTokens = new ArrayList<>(numColumns);
        } else {
            lineTokens = new ArrayList<>();
        }

        // Reset StringBuilder
        sb.setLength(0);
        boolean firstChar = true;
        while(true) {
            int c = bufferedReader.read();
            byteOffset++;

            if (c == -1) {
                throw new EOFException("EOF reached!");
            } else if (firstChar && c == commentChar) {
                // Commented line, skip
                skipLine();
            } else if (c == newLineChar) {
                // Build token, reset StringBuilder, stop reading
                lineTokens.add(sb.toString());
                sb.setLength(0);
                break;
            } else if (c == delimChar) {
                // Build token, reset StringBuilder
                lineTokens.add(sb.toString());
                sb.setLength(0);
            } else {
                // Add char to current StringBuilder
                sb.append((char)c);
            }

            firstChar = false;
        }

        // Check length of tokens against columns
        if (numColumns > 0 && lineTokens.size() != numColumns) {
            throw new DataException("Invalid number of columns in " + fileName + " before byte offset " + String.valueOf(byteOffset));
        }

        // Return tokens as String[]
        return lineTokens.toArray(new String[lineTokens.size()]);
    }

    @Override
    public synchronized SourceRecord readNextRecord(String topic) throws IOException {
        return new SourceRecord(
                sourcePartition,
                getSourceOffset(),
                topic,
                connectSchema,
                csvRecordConverter.convert(readCsvLineIntoTokens()));
    }
}

