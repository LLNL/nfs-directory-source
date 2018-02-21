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

public class LLNLDirectorySourceConnector extends SourceConnector {
    private final String TAG = this.getClass().getSimpleName();
    private static Logger log = LoggerFactory.getLogger(LLNLDirectorySourceConnector.class);

    private LLNLDirectorySourceConfig config;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info(TAG, "start");
        config = new LLNLDirectorySourceConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return LLNLDirectorySourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info(TAG, "taskConfigs");
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(config.originalsStrings());
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        log.info(TAG, "stop");
    }

    @Override
    public ConfigDef config() {
        return LLNLDirectorySourceConfig.conf();
    }
}
