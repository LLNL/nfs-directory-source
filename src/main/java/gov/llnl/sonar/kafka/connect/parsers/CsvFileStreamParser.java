package gov.llnl.sonar.kafka.connect.parsers;

import gov.llnl.sonar.kafka.connect.converters.CsvRecordConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvMapReader;
import org.supercsv.prefs.CsvPreference;

import java.io.*;
import java.util.Map;

@Slf4j
public class CsvFileStreamParser extends FileStreamParser {

    private String filename;
    private Reader fileReader;
    private CsvMapReader reader;

    private CsvRecordConverter csvRecordConverter;
    private String[] header;

    public CsvFileStreamParser(String filename,
                               Schema avroSchema) {

        // TODO: pass in CSV format options here
        super(avroSchema);

        this.filename = filename;

        try {
            fileReader = new FileReader(filename);
            reader = new CsvMapReader(fileReader, CsvPreference.STANDARD_PREFERENCE);

            header = reader.getHeader(true);

            csvRecordConverter = new CsvRecordConverter(connectSchema);
        } catch (FileNotFoundException ex) {
            log.error("File {} not found", filename, ex);
        } catch (IOException e) {
            log.error("IOException:", e);
        }

    }

    @Override
    public Object read() throws EOFException {

        try {
            log.debug("Reading next csv record...");
            Map<String, String> csvRecord = reader.read(header);
            if (csvRecord == null)
                throw new EOFException();

            try {
                log.debug("Building connect record for csv record {}", csvRecord.toString());
                return csvRecordConverter.convert(csvRecord);

            } catch (DataException ex) {
                log.error("Failed to convert csv record {}, from file {}", csvRecord.toString(), filename, ex);
            }
        } catch (SuperCsvException e) {
            log.error("Failed to parse CSV file {} at line {}", filename, reader.getLineNumber());
            log.error("SuperCsvException:", e);
        } catch (IOException e) {
            log.error("IOException:", e);
        }

        return null;
    }

    @Override
    public void skip(Long numRecords) throws EOFException {
        for(Long i = 0L; i < numRecords; i++) {
            try {
                Map<String, String> csvRecord = reader.read(header);
                if (csvRecord == null)
                    throw new EOFException();
            } catch (IOException e) {
                log.error("IOException:", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        fileReader.close();
    }
}

