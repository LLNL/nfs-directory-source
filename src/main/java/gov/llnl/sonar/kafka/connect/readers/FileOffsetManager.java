package gov.llnl.sonar.kafka.connect.readers;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.*;

@Slf4j
public class FileOffsetManager {

    private Long threadID;

    private String fileOffsetBasePath;

    private CuratorFramework client;

    private InterProcessLock lock;

    public FileOffsetManager(String zooKeeperHost, String zooKeeperPort, String fileOffsetBasePath, boolean reset) throws Exception {
        this.threadID = Thread.currentThread().getId();
        this.fileOffsetBasePath = fileOffsetBasePath;

        //log.debug("Thread {}: Initializing zookeeper connection", threadID);

        client = CuratorFrameworkFactory.newClient(
                zooKeeperHost + ":" + zooKeeperPort,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                new RetryForever(1000));

        FileOffsetManager thisRef = this;
        client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                if (!newState.isConnected()) {
                    log.warn("Thread {}: Curator state changed to {} with contents: {}", threadID, newState.toString(), thisRef.toString());
                }
            }
        });
        client.start();
        client.blockUntilConnected();

        //log.debug("Thread {}: Zookeeper connection initialized", threadID);

        if (reset) {
            reset();
        }

        //log.debug("Thread {}: Initializing lock", threadID);
        lock = new InterProcessMutex(client, fileOffsetBasePath);
        //log.debug("Thread {}: Lock initialized", threadID);
    }

    private void reset() throws Exception {

        //log.debug("Thread {}: Checking for file offset base path {}", threadID, fileOffsetBasePath);

        if (client.checkExists().creatingParentContainersIfNeeded().forPath(fileOffsetBasePath) != null) {
            //log.debug("Thread {}: Deleting previous file offset base path {}", threadID, fileOffsetBasePath);
            client.delete().deletingChildrenIfNeeded().forPath(fileOffsetBasePath);
        }

        //log.debug("Thread {}: Creating file offset base path {}", threadID, fileOffsetBasePath);

        client.create()
                .creatingParentContainersIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(fileOffsetBasePath);
    }

    @Override
    public String toString() {
        final String lockString;

        if (lock != null) {
            lockString = String.valueOf(lock.isAcquiredInThisProcess());
        } else {
            lockString = "null";
        }

        return "FileOffsetManager(Path=" + fileOffsetBasePath + ", Locked=" + lockString + ")";
    }

    void uploadFileOffset(String filePath, FileOffset fileOffset) throws Exception {

        //log.debug("Thread {}: Uploading file offset {}: {}", threadID, filePath, fileOffset.toString());
        client.create().orSetData().forPath(filePath, SerializationUtils.serialize(fileOffset));
        //log.debug("Thread {}: Uploaded file offset {}: {}", threadID, filePath, fileOffset.toString());
    }

    /** MUST BE CALLED WITH LOCK */
    FileOffset downloadFileOffsetWithLock(String fileOffsetPath) throws Exception {

        FileOffset fileOffset;

        //log.debug("Thread {}: Downloading file offset if exists: {}", threadID, fileOffsetPath);

        if (client.checkExists().creatingParentContainersIfNeeded().forPath(fileOffsetPath) == null) {
            //log.debug("Thread {}: File offset does not exist, creating and locking it: {} ", threadID, fileOffsetPath);
            fileOffset = new FileOffset(0L, true, false);
            client.create().forPath(fileOffsetPath, SerializationUtils.serialize(fileOffset));
        } else {
            //log.debug("Thread {}: File offset exists, getting it: {} ", threadID, fileOffsetPath);
            byte[] fileOffsetBytes = client.getData().forPath(fileOffsetPath);
            fileOffset = SerializationUtils.deserialize(fileOffsetBytes);
            if (fileOffset.locked || fileOffset.completed) {
                fileOffset = null;
            }
        }

        //log.debug("Thread {}: Downloaded file offset {}: {}", threadID, fileOffsetPath, fileOffset);

        return fileOffset;
    }


    public void lock() {
        try {
            //log.debug("Thread {}: Acquiring lock for {}", threadID, fileOffsetBasePath);
            lock.acquire();
            //log.debug("Thread {}: Acquired lock for {}", threadID, fileOffsetBasePath);
        } catch (Exception e) {
            log.error("Thread {}: {}", threadID, e);
        }
    }

    public void unlock() {
        if(lock.isAcquiredInThisProcess()) {
            try {
                //log.debug("Thread {}: Releasing lock for {}", threadID, fileOffsetBasePath);
                lock.release();
                //log.debug("Thread {}: Released lock for {}", threadID, fileOffsetBasePath);
            } catch (Exception e) {
                log.error("Thread {}: {}", threadID, e);
            }
        }
    }

    public void close() {
        unlock();
        //log.debug("Thread {}: Closing zookeeper client", threadID);
        client.close();
        //log.debug("Thread {}: Closed zookeeper client", threadID);
    }
}
