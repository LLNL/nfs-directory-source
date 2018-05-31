package gov.llnl.sonar.kafka.connect.connectors;

import gov.llnl.sonar.kafka.connect.readers.FileOffsetManager;
import gov.llnl.sonar.kafka.connect.util.BackupUtil;
import gov.llnl.sonar.kafka.connect.util.VersionUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


@Slf4j
public class DirectorySourceConnector extends SourceConnector {

    private DirectorySourceConfig config;
    private FileOffsetManager fileOffsetManager;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {

        // Make dirname absolute
        Path absolutePath = Paths.get(props.get("dirname")).toAbsolutePath();
        String absoluteDirname = absolutePath.toString();
        props.put("dirname", absoluteDirname);

        config = new DirectorySourceConfig(props);

        try {
            // Create new file offset manager in zookeeper
            fileOffsetManager = new FileOffsetManager(
                    config.getZooKeeperHost(),
                    config.getZooKeeperPort(),
                    config.getDirname());

            // Start with empty offset map
            fileOffsetManager.setOffsetMap(new HashMap<>());
            fileOffsetManager.upload();
        } catch (Exception e) {
            log.error("Exception:", e);
        }

        if (config.getBackup()) {
            log.info("Creating backup tarball for ingest directory {}", config.getDirname());
            BackupUtil.createBackupTar(
                    Paths.get(config.getDirname()),
                    Paths.get(config.getCompletedDirname()));
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DirectorySourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Creating {} directory source tasks", maxTasks);

        Path absolutePath = Paths.get(config.getDirname()).toAbsolutePath();

        Map<String, String> configStrings = config.originalsStrings();
        configStrings.put("zk.fileOffsetPath", absolutePath.toString());

        return new ArrayList<>(Collections.nCopies(maxTasks, configStrings));
    }

    @Override
    public void stop() {
        try {
            fileOffsetManager.delete();
            fileOffsetManager.close();
        } catch (Exception e) {
            log.error("Exception:", e);
        }
    }

    @Override
    public ConfigDef config() {
        return DirectorySourceConfig.conf();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config c = super.validate(connectorConfigs);

        List<ConfigValue> configValues = c.configValues();

        // Must have valid dir
        String dirname = connectorConfigs.get("dirname");
        Path dirpath = Paths.get(dirname);

        if (!(  Files.exists(dirpath) &&
                Files.isDirectory(dirpath) &&
                Files.isReadable(dirpath) &&
                Files.isExecutable(dirpath))) {
            for (ConfigValue cv : configValues) {
                if (cv.name().equals("dirname")) {
                    cv.addErrorMessage("Specified \"dirname\" must: exist, be a directory, be readable and executable");
                }
            }
        }

        String completeddirname = connectorConfigs.get("completed.dirname");
        Path completeddirpath = Paths.get(completeddirname);

        if (!(  Files.exists(completeddirpath) &&
                Files.isDirectory(completeddirpath) &&
                Files.isWritable(completeddirpath) &&
                Files.isExecutable(completeddirpath))) {
            for (ConfigValue cv : configValues) {
                if (cv.name().equals("completed.dirname")) {
                    cv.addErrorMessage("Specified \"completed.dirname\" must: exist, be a directory, be writable and executable");
                }
            }
        }

        // Must have avro.schema or avro.schema.filename
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

