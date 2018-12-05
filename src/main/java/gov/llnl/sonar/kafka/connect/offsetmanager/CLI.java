package gov.llnl.sonar.kafka.connect.offsetmanager;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

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
        FileOffsetManager fileOffsetManager = new FileOffsetManager(
                zooKeeperHost,
                String.valueOf(zooKeeperPort),
                ingestDir,
                false);

        FileOffset fileOffset = fileOffsetManager.downloadFileOffsetWithoutLock(ingestFile);

        System.out.println(fileOffset.toString());
    }
}
