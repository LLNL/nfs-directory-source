package gov.llnl.sonar.kafka.connect.connectors;

import gov.llnl.sonar.kafka.connect.readers.FileOffsetManager;
import gov.llnl.sonar.kafka.connect.util.VersionUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


@Slf4j
public class DirectorySourceConnector extends SourceConnector {

    private String taskID;
    private DirectorySourceConfig config;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {

        // Make dirname absolute
        Path absolutePath = Paths.get(props.get(DirectorySourceConfig.DIRNAME)).toAbsolutePath();
        String absoluteDirname = absolutePath.toString();
        props.put(DirectorySourceConfig.DIRNAME, absoluteDirname);

        config = new DirectorySourceConfig(props);

        try {
            // Get task ID
            this.taskID = InetAddress.getLocalHost().getHostName() + "(" + Thread.currentThread().getId() + ")";
            log.info("Connector task {}: Start", taskID);
        } catch (Exception e) {
            log.error("Exception:", e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DirectorySourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Connector task {}: Creating {} directory source tasks", taskID, maxTasks);

        Path absolutePath = Paths.get(config.getDirname()).toAbsolutePath();

        Map<String, String> configStrings = config.originalsStrings();
        configStrings.put("zk.fileOffsetPath", absolutePath.toString());

        return new ArrayList<>(Collections.nCopies(maxTasks, configStrings));
    }

    @Override
    public void stop() {
        log.info("Connector task {}: Stop", taskID);
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
        String dirname = connectorConfigs.get(DirectorySourceConfig.DIRNAME);
        Path dirpath = Paths.get(dirname);

        if (!(  Files.exists(dirpath) &&
                Files.isDirectory(dirpath) &&
                Files.isReadable(dirpath) &&
                Files.isExecutable(dirpath))) {
            for (ConfigValue cv : configValues) {
                if (cv.name().equals(DirectorySourceConfig.DIRNAME)) {
                    cv.addErrorMessage("Specified \"" + DirectorySourceConfig.DIRNAME +  "\" must: exist, be a directory, be readable and executable");
                }
            }
        }

        String completeddirname = connectorConfigs.get(DirectorySourceConfig.COMPLETED_DIRNAME);
        Path completeddirpath = Paths.get(completeddirname);

        if (!(  Files.exists(completeddirpath) &&
                Files.isDirectory(completeddirpath) &&
                Files.isWritable(completeddirpath) &&
                Files.isExecutable(completeddirpath))) {
            for (ConfigValue cv : configValues) {
                if (cv.name().equals(DirectorySourceConfig.COMPLETED_DIRNAME)) {
                    cv.addErrorMessage("Specified \"" + DirectorySourceConfig.DIRNAME + "\" must: exist, be a directory, be writable and executable");
                }
            }
        }

        // Must have avro.schema or avro.schema.filename
        if (connectorConfigs.containsKey(DirectorySourceConfig.AVRO_SCHEMA) == connectorConfigs.containsKey(DirectorySourceConfig.AVRO_SCHEMA_FILENAME)) {
            for (ConfigValue cv : configValues) {
                if (cv.name().equals(DirectorySourceConfig.AVRO_SCHEMA)) {
                    cv.addErrorMessage("Connector requires either \"" + DirectorySourceConfig.AVRO_SCHEMA + "\" or \"" + DirectorySourceConfig.AVRO_SCHEMA_FILENAME + "\" (and not both)!");
                }
            }
        }

        // Hacking into here since this runs only once when a connector is started
        try {
            log.info("{}: creating file offset manager", this.getClass());

            // Get configs
            Path absolutePath = Paths.get(connectorConfigs.get(DirectorySourceConfig.DIRNAME)).toAbsolutePath();
            String absoluteDirname = absolutePath.toString();
            String zooKeeperHost = connectorConfigs.get(DirectorySourceConfig.ZKHOST);
            String zooKeeperPort = connectorConfigs.get(DirectorySourceConfig.ZKPORT);

            // Create new file offset manager in zookeeper with empty offset map
            FileOffsetManager fileOffsetManager = new FileOffsetManager(zooKeeperHost, zooKeeperPort, absoluteDirname);
            fileOffsetManager.setOffsetMap(new HashMap<>());
            fileOffsetManager.upload();
            fileOffsetManager.close();
        } catch (Exception e) {
            log.error("Exception:", e);
        }


        return new Config(configValues);
    }
}

