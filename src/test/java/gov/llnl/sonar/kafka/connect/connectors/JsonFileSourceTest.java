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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static gov.llnl.sonar.kafka.connect.connectors.ConnectTestData.*;

@Log4j
public class JsonFileSourceTest extends ConnectTest {

    Map<String, String> configProperties = new HashMap<>();

    private Path jsonTestFile;
    private String jsonTestSourceConnector;
    private String jsonTestSourceTopic;

    @Before
    public void setup() {
        super.setup();

        try {
            log.info("Creating test JSON file");
            jsonTestFile = Files.createTempFile("json-test-file-source-", ".json");

            log.info("Writing JSON entries to file source");
            BufferedWriter bw = new BufferedWriter(new FileWriter(jsonTestFile.toFile()));
            bw.write("{\"id\": 1, \"str\": \"one\"}\n");
            bw.write("{\"id\": 2, \"str\": \"two\"}\n");
            bw.write("{\"id\": 3, \"str\": \"three\"}\n");
            bw.write("{\"id\": 4, \"str\": \"four\"}\n");
            bw.write("{\"id\": 5, \"str\": \"five\"}\n");
            bw.write("{\"id\": 6, \"str\": \"six\"}\n");
            bw.write("{\"id\": 7, \"str\": \"seven\"}\n");
            bw.write("{\"id\": 8, \"str\": \"eight\"}\n");
            bw.flush();
        } catch (IOException ex) {
            log.error(ex);
        }

        String jsonTestFilename = jsonTestFile.toString();
        String jsonTestFileBasename = FilenameUtils.getBaseName(jsonTestFilename);
        jsonTestSourceConnector = jsonTestFileBasename;
        jsonTestSourceTopic = jsonTestFileBasename + "-topic";

        configProperties.put(FileSourceConfig.FILENAME, jsonTestFilename);
        configProperties.put(FileSourceConfig.FORMAT, "json");
        configProperties.put(FileSourceConfig.FORMAT_OPTIONS, "");
        configProperties.put(FileSourceConfig.TOPIC, jsonTestSourceTopic);
        configProperties.put(FileSourceConfig.AVRO_SCHEMA, idstrAvroSchemaEscapedString);

    }

    @Test
    public void testFileSourceJson() throws IOException {

        log.info("Creating connector " + jsonTestSourceConnector);
        log.info(confluent.createConnector(jsonTestSourceConnector, FileSourceConnector.class, configProperties));

        validateTopicContents(jsonTestSourceTopic, idstrAvroData);
    }

    @After
    public void teardown() {
        confluent.deleteConnector(jsonTestSourceConnector);
        confluent.deleteTopic(jsonTestSourceTopic);
        jsonTestFile.toFile().delete();
        super.teardown();
    }

}