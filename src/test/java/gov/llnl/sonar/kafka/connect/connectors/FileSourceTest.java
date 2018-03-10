package gov.llnl.sonar.kafka.connect.connectors;

import lombok.extern.log4j.Log4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

@Log4j
public class FileSourceTest extends EasyMockSupport {

    private ConfluentDriver confluent;

    private File csvTestFile;
    private String csvTestSourceName = "test-file-source-csv";
    private String csvTestSourceTopic = "test-file-source-csv-topic";

    private File jsonTestFile;
    private String jsonTestSourceName = "test-file-source-json";
    private String jsonTestSourceTopic = "test-file-source-json-topic";

    private String idstrAvroSchemaString = "{" +
            "  \"type\": \"record\"," +
            "  \"name\": \"idstr\"," +
            "  \"connect.name\": \"idstr\"," +
            "  \"fields\": [" +
            "    {" +
            "      \"name\": \"id\"," +
            "      \"type\": \"int\"" +
            "    }," +
            "    {" +
            "      \"name\": \"str\"," +
            "      \"type\": \"string\"" +
            "    }" +
            "  ]" +
            "}";

    Schema idstrAvroSchema = new Schema.Parser().parse(idstrAvroSchemaString);

    List<String> testRecords = Arrays.asList(
            "{\"id\": 1, \"str\": \"one\"}",
            "{\"id\": 2, \"str\": \"two\"}"
    );

    @Before
    public void setup() throws IOException {
        confluent = new ConfluentDriver("/Users/gimenez1/Home/local/src/confluent-4.0.0/bin");
        csvTestFile = File.createTempFile("file-source-test", ".csv");
        jsonTestFile = File.createTempFile("file-source-test", ".json");
    }

    private void validateTopicContents(String topic) {
        log.info("Consuming topic " + topic);
        Consumer consumer = confluent.createConsumer(topic);
        Iterable<ConsumerRecord> consumerStream = consumer.poll(10000);
        List<String> consumedRecords = new ArrayList<>();
        for (ConsumerRecord consumerRecord : consumerStream) {
            GenericData.Record record = (GenericData.Record) consumerRecord.value();
            log.info("<<< Consumed record: " + record.toString());
            consumedRecords.add(record.toString());
            assertEquals(idstrAvroSchema, record.getSchema());
        }

        assertEquals(testRecords, consumedRecords);
    }

    @Test
    public void testFileSourceCsv() throws IOException {

        log.info("Writing entries to file source");
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


        log.info("Creating connector " + csvTestSourceName);
        Map<String, String> configProperties = new HashMap<>();
        configProperties.put(FileSourceConfig.FILENAME, csvTestFile.getCanonicalPath());
        configProperties.put(FileSourceConfig.FORMAT, "csv");
        configProperties.put(FileSourceConfig.FORMAT_OPTIONS, "");
        configProperties.put(FileSourceConfig.TOPIC, csvTestSourceTopic);
        configProperties.put(FileSourceConfig.AVRO_SCHEMA, idstrAvroSchemaString.replaceAll("\"", "\\\""));
        log.info(confluent.createConnector(csvTestSourceName, FileSourceConnector.class, configProperties));

        validateTopicContents(csvTestSourceTopic);
    }
    @Test
    public void testFileSourceJson() throws IOException {

        log.info("Writing entries to file source");
        BufferedWriter bw = new BufferedWriter(new FileWriter(jsonTestFile));
        for (String r : testRecords) {
            bw.write(r + "\n");
        }
        bw.flush();

        log.info("Creating connector " + jsonTestSourceName);
        Map<String, String> configProperties = new HashMap<>();
        configProperties.put(FileSourceConfig.FILENAME, jsonTestFile.getCanonicalPath());
        configProperties.put(FileSourceConfig.FORMAT, "json");
        configProperties.put(FileSourceConfig.FORMAT_OPTIONS, "");
        configProperties.put(FileSourceConfig.TOPIC, jsonTestSourceTopic);
        configProperties.put(FileSourceConfig.AVRO_SCHEMA, idstrAvroSchemaString.replaceAll("\"", "\\\""));
        log.info(confluent.createConnector(jsonTestSourceName, FileSourceConnector.class, configProperties));

        validateTopicContents(jsonTestSourceTopic);
    }

    @After
    public void teardown() {

        try {
            log.info("Deleting topic " + jsonTestSourceTopic);
            confluent.deleteTopic(jsonTestSourceTopic);
        } catch (IllegalArgumentException ex) {
            log.error(ex);
        }

        log.info("Deleting connector " + jsonTestSourceName);
        log.info(confluent.deleteConnector(jsonTestSourceName));

        log.info("Deleting temp file " + jsonTestFile.getName());
        jsonTestFile.delete();

        confluent.close();
    }

}