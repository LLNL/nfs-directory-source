package com.github.llnl.kafka.connectors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LLNLFileSourceConnector extends SourceConnector {
    private static Logger log = LoggerFactory.getLogger(LLNLFileSourceConnector.class);

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
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(config.originalsStrings());
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() { }

    @Override
    public ConfigDef config() {
        return LLNLFileSourceConfig.conf();
    }
}
