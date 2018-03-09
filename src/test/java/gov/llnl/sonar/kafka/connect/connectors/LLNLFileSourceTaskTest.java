package gov.llnl.sonar.kafka.connect.connectors;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LLNLFileSourceTaskTest extends EasyMockSupport {
    private static final String TOPIC = "test";

    private File tempFile;
    private Map<String, String> config;
    private OffsetStorageReader offsetStorageReader;
    private SourceTaskContext context;
    private LLNLFileSourceTask task;

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

    private boolean verifyMocks = false;

    @Before
    public void setup() throws IOException {
        tempFile = File.createTempFile("llnl-file-source-task-test", null);
        config = new HashMap<>();
        config.put(LLNLFileSourceConfig.FILENAME, tempFile.getAbsolutePath());
        config.put(LLNLFileSourceConfig.TOPIC, TOPIC);
        config.put(LLNLFileSourceConfig.FORMAT, "json");
        config.put(LLNLFileSourceConfig.AVRO_SCHEMA, avroSchema);
        task = new LLNLFileSourceTask();
        task.initialize(context);
    }

    @After
    public void teardown() {
        tempFile.delete();

        if (verifyMocks)
            verifyAll();
    }

    private void replay() {
        replayAll();
        verifyMocks = true;
    }

    // TODO: make some real tests here

    @Test(expected = ConfigException.class)
    public void testMissingTopic() throws InterruptedException {
        replay();

        config.remove(LLNLFileSourceConfig.TOPIC);
        task.start(config);
    }

    @Test(expected = ConfigException.class)
    public void testMissingFile() throws InterruptedException {
        replay();

        config.remove(LLNLFileSourceConfig.FILENAME);
        task.start(config);
    }
}