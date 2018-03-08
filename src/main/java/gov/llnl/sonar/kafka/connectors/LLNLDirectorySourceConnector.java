package gov.llnl.sonar.kafka.connectors;

import java.util.*;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LLNLDirectorySourceConnector extends SourceConnector {
    private static Logger log = LoggerFactory.getLogger(LLNLDirectorySourceConnector.class);

    private LLNLDirectorySourceConfig config;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new LLNLDirectorySourceConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return LLNLDirectorySourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return new ArrayList<>(Collections.nCopies(maxTasks, config.originalsStrings()));
    }

    @Override
    public void stop() { }

    @Override
    public ConfigDef config() {
        return LLNLDirectorySourceConfig.conf();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config c = super.validate(connectorConfigs);

        List<ConfigValue> configValues = c.configValues();
        if (!(connectorConfigs.containsKey("avro.schema") ^ connectorConfigs.containsKey("avro.schema.filename"))) {
            for (ConfigValue cv : configValues) {
                if (cv.name().equals("avro.schema")) {
                    cv.addErrorMessage("Connector requires either avro.schema or avro.schema.filename (and not both)!");
                }
            }
        }

        return new Config(configValues);
    }
}
