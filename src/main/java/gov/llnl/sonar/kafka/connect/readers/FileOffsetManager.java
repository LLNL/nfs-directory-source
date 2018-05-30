package gov.llnl.sonar.kafka.connect.readers;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.*;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FileOffsetManager {

    private HashMap<String, FileOffset> fileOffsetMap = new HashMap<>();

    private String zooKeeperPath;

    CuratorFramework client;

    private InterProcessLock lock;

    public FileOffsetManager(String zooKeeperHostname, String fileOffsetPath) throws Exception {
        this.zooKeeperPath = fileOffsetPath;

        client = CuratorFrameworkFactory.newClient(zooKeeperHostname, new ExponentialBackoffRetry(1000, 3));
        client.start();
        client.blockUntilConnected();

        lock = new InterProcessMutex(client, fileOffsetPath);

        // Create offset manager exactly once if necessary
        synchronized (FileOffsetManager.class) {
            lock();

            if (client.checkExists().creatingParentContainersIfNeeded().forPath(fileOffsetPath) == null) {
                // Path DNE, create it
                client.create()
                        .creatingParentContainersIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                        .forPath(this.zooKeeperPath, SerializationUtils.serialize(fileOffsetMap));

                // Upload empty map
                upload();
            }

            unlock();
        }

    }

    public void upload() throws Exception {
        client.setData().forPath(zooKeeperPath, SerializationUtils.serialize(fileOffsetMap));
    }

    public void download() throws Exception {
        byte[] fileOffsetMapBytes = client.getData().forPath(zooKeeperPath);
        fileOffsetMap = SerializationUtils.deserialize(fileOffsetMapBytes);
    }

    public void delete() throws Exception {

        synchronized (FileOffsetManager.class) {

            lock();

            if (client.checkExists().forPath(zooKeeperPath) == null) {
                client.delete().forPath(zooKeeperPath);
            }

            unlock();
        }
    }

    public HashMap<String, FileOffset> getOffsetMap() {
        return fileOffsetMap;
    }

    public void setOffsetMap(HashMap<String, FileOffset> map) {
        fileOffsetMap = map;
    }

    public boolean lock() {
        try {
            return lock.acquire(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Exception:", e);
        }
        return false;
    }

    public boolean unlock() {
        try {
            lock.release();
            return true;
        } catch (Exception e) {
            log.error("Exception:", e);
        }
        return false;
    }

    public void close() throws Exception {
        if (lock.isAcquiredInThisProcess()) {
            unlock();
        }
        client.close();
    }
}
