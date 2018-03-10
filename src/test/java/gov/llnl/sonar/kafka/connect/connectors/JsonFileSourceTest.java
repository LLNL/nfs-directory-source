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
public class JsonFileSourceTest extends ConnectTest {

    Map<String, String> configProperties = new HashMap<>();

    private File jsonTestFile;
    private static final String jsonTestSourceConnector = "test-file-source-json";
    private static final String jsonTestSourceTopic = "test-file-source-json-topic";

    @Override
    List<String> topics() {
        return Arrays.asList(jsonTestSourceTopic);
    }

    @Override
    List<String> connectors() {
        return Arrays.asList(jsonTestSourceConnector);
    }

    @Before
    public void setup() {
        super.setup();

        try {
            log.info("Creating test JSON file");
            jsonTestFile = File.createTempFile("file-source-test", ".json");

            log.info("Writing JSON entries to file source");
            BufferedWriter bw = new BufferedWriter(new FileWriter(jsonTestFile));
            for (String r : testRecords) {
                bw.write(r + "\n");
            }
            bw.flush();
        } catch (IOException ex) {
            log.error(ex);
        }

        configProperties.put(FileSourceConfig.FILENAME, jsonTestFile.getAbsolutePath());
        configProperties.put(FileSourceConfig.FORMAT, "json");
        configProperties.put(FileSourceConfig.FORMAT_OPTIONS, "");
        configProperties.put(FileSourceConfig.TOPIC, jsonTestSourceTopic);
        configProperties.put(FileSourceConfig.AVRO_SCHEMA, idstrAvroSchemaEscapedString);

    }

    @Test
    public void testFileSourceJson() throws IOException {

        log.info("Creating connector " + jsonTestSourceConnector);
        log.info(confluent.createConnector(jsonTestSourceConnector, FileSourceConnector.class, configProperties));

        validateTopicContents(confluent, jsonTestSourceTopic);
    }

    @After
    public void teardown() {
        super.teardown();
        jsonTestFile.delete();
    }

}