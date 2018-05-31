package gov.llnl.sonar.kafka.connect.readers;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.*;

import java.io.EOFException;
import java.util.HashMap;

@Slf4j
public class FileOffsetManager {

    private HashMap<String, FileOffset> fileOffsetMap = new HashMap<>();

    private String zooKeeperPath;

    CuratorFramework client;

    private InterProcessLock lock;

    public FileOffsetManager(String zooKeeperHost, String zooKeeperPort, String fileOffsetPath) throws Exception {
        this.zooKeeperPath = fileOffsetPath;

        client = CuratorFrameworkFactory.newClient(zooKeeperHost + ":" + zooKeeperPort, new ExponentialBackoffRetry(1000, 3));
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

        try {
            fileOffsetMap = SerializationUtils.deserialize(fileOffsetMapBytes);
        } catch (SerializationException e) {
            if (ExceptionUtils.getRootCause(e) instanceof EOFException) {
                // empty file offset map, that's ok
            } else {
                throw e;
            }
        }
    }

    public void delete() throws Exception {

        synchronized (FileOffsetManager.class) {

            lock();

            if (client.checkExists().forPath(zooKeeperPath) != null) {
                client.delete().deletingChildrenIfNeeded().forPath(zooKeeperPath);
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
            lock.acquire();
            return true;
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

    public void close() {
        if (lock.isAcquiredInThisProcess()) {
            unlock();
        }
        client.close();
    }
}
