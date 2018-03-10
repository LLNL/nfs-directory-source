package gov.llnl.sonar.kafka.connect.connectors;

import lombok.extern.log4j.Log4j;
import org.apache.commons.io.FilenameUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static gov.llnl.sonar.kafka.connect.connectors.ConnectTestData.*;

@Log4j
public class CsvFileSourceTest extends ConnectTest {

    private File csvTestFile;
    private String csvTestSourceConnector;
    private String csvTestSourceTopic;

    private Map<String, String> configProperties = new HashMap<>();

    @Before
    public void setup() {

        super.setup();

        try {
            log.info("Creating test CSV file");
            csvTestFile = File.createTempFile("csv-test-file-source-", ".csv");

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

        String csvTestFilename = csvTestFile.getAbsolutePath();
        String csvTestFileBasename = FilenameUtils.getBaseName(csvTestFilename);
        csvTestSourceConnector = csvTestFileBasename;
        csvTestSourceTopic = csvTestFileBasename + "-topic";

        configProperties.put(FileSourceConfig.FILENAME, csvTestFilename);
        configProperties.put(FileSourceConfig.FORMAT, "csv");
        configProperties.put(FileSourceConfig.FORMAT_OPTIONS, "");
        configProperties.put(FileSourceConfig.TOPIC, csvTestSourceTopic);
        configProperties.put(FileSourceConfig.AVRO_SCHEMA, idstrAvroSchemaEscapedString);

    }

    @Test
    public void testFileSourceCsv() throws IOException {

        log.info("Creating connector " + csvTestSourceConnector);
        log.info(confluent.createConnector(csvTestSourceConnector, FileSourceConnector.class, configProperties));

        validateTopicContents(csvTestSourceTopic, idstrAvroData);
    }

    @After
    public void teardown() {
        confluent.deleteConnector(csvTestSourceConnector);
        confluent.deleteTopic(csvTestSourceTopic);
        csvTestFile.delete();
        super.teardown();
    }

}

