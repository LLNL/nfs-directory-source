package gov.llnl.sonar.kafka.connectors;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LLNLFileSourceConnectorTest extends EasyMockSupport {
    private static final String SINGLE_TOPIC = "test";
    private static final String FILENAME = "/somefilename";

    private LLNLFileSourceConnector connector;
    private ConnectorContext ctx;
    private Map<String, String> sourceProperties;

    @Before
    public void setup() {
        connector = new LLNLFileSourceConnector();
        ctx = createMock(ConnectorContext.class);
        connector.initialize(ctx);

        sourceProperties = new HashMap<>();
        sourceProperties.put(LLNLFileSourceConfig.TOPIC, SINGLE_TOPIC);
        sourceProperties.put(LLNLFileSourceConfig.FILENAME, FILENAME);
        sourceProperties.put(LLNLFileSourceConfig.FORMAT, "json");
    }

    @Test
    public void testSourceTasks() {
        replayAll();

        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertEquals(FILENAME,
                taskConfigs.get(0).get(LLNLFileSourceConfig.FILENAME));
        assertEquals(SINGLE_TOPIC,
                taskConfigs.get(0).get(LLNLFileSourceConfig.TOPIC));

        taskConfigs = connector.taskConfigs(2);
        assertEquals(2, taskConfigs.size());
        assertEquals(FILENAME,
                taskConfigs.get(0).get(LLNLFileSourceConfig.FILENAME));
        assertEquals(SINGLE_TOPIC,
                taskConfigs.get(0).get(LLNLFileSourceConfig.TOPIC));

        verifyAll();
    }

    @Test(expected = ConfigException.class)
    public void testNoFilename() {
        EasyMock.replay(ctx);

        sourceProperties.remove(LLNLFileSourceConfig.FILENAME);
        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertNull(taskConfigs.get(0).get(LLNLFileSourceConfig.FILENAME));

        EasyMock.verify(ctx);
    }

    @Test
    public void testTaskClass() {
        EasyMock.replay(ctx);

        connector.start(sourceProperties);
        assertEquals(LLNLFileSourceTask.class, connector.taskClass());

        EasyMock.verify(ctx);
    }
}
