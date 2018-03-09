package gov.llnl.sonar.kafka.connect.connectors;

import gov.llnl.sonar.kafka.connect.util.VersionUtil;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LLNLFileSourceConnector extends SourceConnector {

    private LLNLFileSourceConfig config;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new LLNLFileSourceConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return LLNLFileSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return new ArrayList<>(Collections.nCopies(maxTasks, config.originalsStrings()));
    }

    @Override
    public void stop() { }

    @Override
    public ConfigDef config() {
        return LLNLFileSourceConfig.conf();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config c = super.validate(connectorConfigs);

        List<ConfigValue> configValues = c.configValues();
        if (connectorConfigs.containsKey("avro.schema") == connectorConfigs.containsKey("avro.schema.filename")) {
            for (ConfigValue cv : configValues) {
                if (cv.name().equals("avro.schema")) {
                    cv.addErrorMessage("Connector requires either avro.schema or avro.schema.filename (and not both)!");
                }
            }
        }

        return new Config(configValues);
    }
}
