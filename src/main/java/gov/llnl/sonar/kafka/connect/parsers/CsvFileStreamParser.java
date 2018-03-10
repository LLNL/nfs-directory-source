package gov.llnl.sonar.kafka.connect.parsers;

import gov.llnl.sonar.kafka.connect.converters.CsvRecordConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.io.*;
import java.util.Iterator;
import java.util.Map;

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
        } catch (IOException ex) {
            log.error("Exception:", ex);
        }

    }

    @Override
    public Object read() throws EOFException {

        if (csvRecordIterator.hasNext()) {

            // TODO: how to catch when csv record parse fails?
            log.info("Reading next csv record...");
            CSVRecord csvRecord = csvRecordIterator.next();

            try {

                log.info("Building connect record for csv record {}", csvRecord.toString());
                return csvRecordConverter.convert(csvRecord);

            } catch (DataException ex) {
                log.error("Failed to parse file {}, row {}", filename, csvRecord.toString(), ex);
            }
        } else {
            throw new EOFException();
        }

        return null;
    }

    @Override
    public void close() throws IOException {
        fileReader.close();
    }
}

