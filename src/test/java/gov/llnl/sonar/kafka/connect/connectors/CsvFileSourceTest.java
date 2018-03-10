package gov.llnl.sonar.kafka.connect.connectors;

import lombok.extern.log4j.Log4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static gov.llnl.sonar.kafka.connect.connectors.TestData.*;

@Log4j
public class CsvFileSourceTest extends ConnectTest {

    private File csvTestFile;
    private static final String csvTestSourceConnector = "test-file-source-csv";
    private static final String csvTestSourceTopic = "test-file-source-csv-topic";

    private Map<String, String> configProperties = new HashMap<>();

    @Override
    List<String> topics() {
        return Arrays.asList(csvTestSourceTopic);
    }

    @Override
    List<String> connectors() {
        return Arrays.asList(csvTestSourceConnector);
    }

    @Before
    public void setup() {

        super.setup();

        try {
            log.info("Creating test CSV file");
            csvTestFile = File.createTempFile("file-source-test", ".csv");

            log.info("Writing CSV entries to file source");
            BufferedWriter bw = new BufferedWriter(new FileWriter(csvTestFile));
            List<String> inputRecords = Arrays.asList(
                    "\"id\",\"str\"",
                    "\"1\",\"one\"",
                    "\"2\",\"two\""
            );
            for (String r : inputRecords) {
                bw.write(r + "\n");
            }
            bw.flush();

        } catch (IOException ex) {
            log.error(ex);
        }

        configProperties.put(FileSourceConfig.FILENAME, csvTestFile.getAbsolutePath());
        configProperties.put(FileSourceConfig.FORMAT, "csv");
        configProperties.put(FileSourceConfig.FORMAT_OPTIONS, "");
        configProperties.put(FileSourceConfig.TOPIC, csvTestSourceTopic);
        configProperties.put(FileSourceConfig.AVRO_SCHEMA, idstrAvroSchemaEscapedString);

    }

    @Test
    public void testFileSourceCsv() throws IOException {

        log.info("Creating connector " + csvTestSourceConnector);
        log.info(confluent.createConnector(csvTestSourceConnector, FileSourceConnector.class, configProperties));

        validateTopicContents(confluent, csvTestSourceTopic);
    }

    @After
    public void teardown() {
        super.teardown();
        csvTestFile.delete();
    }

}

