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
    private static Logger log = LoggerFactory.getLogger(LLNLDirectorySourceConnector.class);
    public static final String TAG = "LLNLDirectorySourceConnector";

    public static final String FILENAME_CONFIG = "filename";
    public static final String TOPIC_CONFIG = "topic";

    private String filename;
    private String topic;

    private LLNLDirectorySourceConfig config;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info(TAG, "connector start");
        config = new LLNLDirectorySourceConfig(props);
        filename = config.getFilename();
        topic = config.getTopic();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return LLNLDirectorySourceTask.class;
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
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return LLNLDirectorySourceConfig.conf();
    }
}
