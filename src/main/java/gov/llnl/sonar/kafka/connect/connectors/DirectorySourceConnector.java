package gov.llnl.sonar.kafka.connect.connectors;

import gov.llnl.sonar.kafka.connect.util.BackupUtil;
import gov.llnl.sonar.kafka.connect.util.VersionUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.nio.file.StandardOpenOption.*;

@Slf4j
public class DirectorySourceConnector extends SourceConnector {

    private DirectorySourceConfig config;

    private FileChannel pathWalkLockFile;

    public final static String LOCK_FILENAME = ".directory-walker.lock";

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new DirectorySourceConfig(props);

        // BackupUtil.createBackupTar(
        //         Paths.get(config.getDirname()),
        //         Paths.get(config.getCompletedDirname()));

        Path pathWalkLockPath = Paths.get(config.getCompletedDirname(), LOCK_FILENAME);

        try {
            pathWalkLockFile = FileChannel.open(pathWalkLockPath, READ, WRITE, CREATE_NEW, SYNC);
        } catch (IOException e) {
            log.info("Path lock file {} already created, using it", pathWalkLockPath.toString());
            pathWalkLockFile = null;
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DirectorySourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Creating {} directory source tasks", maxTasks);
        return new ArrayList<>(Collections.nCopies(maxTasks, config.originalsStrings()));
    }

    @Override
    public void stop() {
        try {
            if (pathWalkLockFile != null) {
                pathWalkLockFile.close();
            }
        } catch (IOException e) {
            log.error("IOException opening path lock file {}", pathWalkLockFile.toString(), e);
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

        Path pathlockpath = Paths.get(completeddirname, ".directory-walker-lock");

        if (Files.exists(pathlockpath)) {
            for (ConfigValue cv : configValues) {
                if (cv.name().equals("completed.dirname")) {
                    cv.addErrorMessage("\"completed.dirname\" contains a path lock file (.directory-walker-lock), remove it or specify a new completed.dirname");
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

