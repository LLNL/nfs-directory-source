package gov.llnl.sonar.kafka.connect.offsetmanager;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryForever;

import static gov.llnl.sonar.kafka.connect.offsetmanager.FileOffsetManager.makeOffsetPath;

public class CLI {

    @Parameter(names={"--zk-host", "-h"})
    String zooKeeperHost = "localhost";

    @Parameter(names={"--zk-port", "-p"})
    int zooKeeperPort = 2181;

    @Parameter(names={"--ingest-dir", "-d"})
    String ingestDir;

    @Parameter(names={"--ingest-file", "-f"})
    String ingestFile;

    public static void main(String... argv) {
        CLI cli = new CLI();
        JCommander.newBuilder()
                .addObject(cli)
                .build()
                .parse(argv);

        try {
            cli.run();
        } catch (Exception e) {
            System.err.println(e.toString());
        }
    }

    public void run() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                zooKeeperHost + ":" + zooKeeperPort,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                new RetryForever(1000));

        client.start();
        client.blockUntilConnected();

        FileOffset fileOffset;
        String actualFileOffsetPath = makeOffsetPath(ingestDir, ingestFile);

        byte[] fileOffsetBytes = client.getData().forPath(actualFileOffsetPath);
        fileOffset = SerializationUtils.deserialize(fileOffsetBytes);

        System.out.println(fileOffset.toString());
    }
}
