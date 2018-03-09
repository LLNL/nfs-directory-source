package gov.llnl.sonar.kafka.connect.connectors;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertEquals;
import static org.easymock.EasyMock.replay;

public class FileSourceTest extends EasyMockSupport {
    private static final String TOPIC = "test";

    private File tempFile;
    private FileSourceConnector connector;
    private ConnectorContext context;
    private Map<String, String> configProperties;
    private FileSourceTask task;

    private String avroSchema = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"idstr\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"id\",\n" +
            "      \"type\": \"int\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"str\",\n" +
            "      \"type\": \"string\"\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    @Before
    public void setup() throws IOException {
        connector = new FileSourceConnector();
        context = createMock(ConnectorContext.class);
        connector.initialize(context);

        tempFile = File.createTempFile("file-source-task-test", null);
        configProperties = new HashMap<>();
        configProperties.put(FileSourceConfig.FILENAME, tempFile.getAbsolutePath());
        configProperties.put(FileSourceConfig.TOPIC, TOPIC);
        configProperties.put(FileSourceConfig.FORMAT, "json");
        configProperties.put(FileSourceConfig.AVRO_SCHEMA, avroSchema);
        task = new FileSourceTask();
    }

    @After
    public void teardown() {
        tempFile.delete();
    }

    @Test
    public void testTaskClass() {
        replay();

        connector.start(configProperties);
        assertEquals(FileSourceTask.class, connector.taskClass());
    }

    @Test(expected = ConfigException.class)
    public void testMissingTopic() throws InterruptedException {
        replay();

        configProperties.remove(FileSourceConfig.TOPIC);
        task.start(configProperties);
    }

    @Test(expected = ConfigException.class)
    public void testMissingFile() throws InterruptedException {
        replay();

        configProperties.remove(FileSourceConfig.FILENAME);
        task.start(configProperties);
    }
}