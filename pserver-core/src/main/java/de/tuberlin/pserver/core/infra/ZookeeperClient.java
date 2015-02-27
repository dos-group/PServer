package de.tuberlin.pserver.core.infra;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.*;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperClient {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String IM_EVENT_NODE_ADDED = "IM_EVENT_NODE_ADDED";

    public static final String IM_EVENT_NODE_REMOVED = "IM_EVENT_NODE_REMOVED";

    public static final String EVENT_TYPE_CONNECTION_ESTABLISHED = "EVENT_TYPE_CONNECTION_ESTABLISHED";

    public static final String EVENT_TYPE_CONNECTION_EXPIRED = "EVENT_TYPE_CONNECTION_EXPIRED";

    // ---------------------------------------------------

    public static final String ZOOKEEPER_ROOT = "/pserver";

    public static final String ZOOKEEPER_NODES = ZOOKEEPER_ROOT + "/nodes";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperClient.class);

    private final CuratorFramework curator;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public ZookeeperClient(final String zookeeperServer) {
        Preconditions.checkNotNull(zookeeperServer);
        curator = CuratorFrameworkFactory.newClient(zookeeperServer, 60000 * 10, 60000 * 10, new ExponentialBackoffRetry(1000, 3));
        curator.start();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void initDirectories() throws Exception {
        // Create the root folder of the aura application in ZooKeeper.
        Stat stat = curator.checkExists().forPath(ZOOKEEPER_ROOT);
        if (stat == null) {
            try {
                curator.create().forPath(ZOOKEEPER_ROOT, new byte[0]);
            } catch (KeeperException.NodeExistsException e) {
                // These nodes only need to exist. No matter who created them.
            }
        }
        stat = curator.checkExists().forPath(ZOOKEEPER_NODES);
        if (stat == null) {
            try {
                curator.create().forPath(ZOOKEEPER_NODES, new byte[0]);
            } catch (KeeperException.NodeExistsException e) {
                // These nodes only need to exist. No matter who created them.
            }
        }
    }

    public Object read(final String path) throws Exception {
        try {
            final byte[] data = curator.getData().forPath(path);
            final ByteArrayInputStream byteArrayStream = new ByteArrayInputStream(data);
            final ObjectInputStream objectStream = new ObjectInputStream(byteArrayStream);
            return objectStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    public void store(final String path, final Object object) throws Exception {
        try {
            final ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
            final ObjectOutputStream objectStream = new ObjectOutputStream(byteArrayStream);
            objectStream.writeObject(object);
            objectStream.flush();
            curator.create().withMode(CreateMode.PERSISTENT).forPath(path, byteArrayStream.toByteArray());
            objectStream.close();
            byteArrayStream.close();
        } catch (InterruptedException e) {
            LOG.error(e.getLocalizedMessage());
        }
    }

    public List<String> getChildrenForPath(String path) {
        try {
            return curator.getChildren().forPath(path);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public List<String> getChildrenForPathAndWatch(String path, Watcher watcher) {
        try {
            return curator.getChildren().usingWatcher(watcher).forPath(path);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void close() {
        curator.close();
    }

    public static void checkConnectionString(String zkServer) {
        checkNotNull(zkServer, "zkServers == null");
        final String[] tokens = zkServer.split(";");
        for (String token : tokens) {
            final String[] parts = token.split(":");
            try {
                final int port = Integer.parseInt(parts[1]);
                checkArgument(port > 1024 && port < 65535, "Port {} is invalid", port);
            } catch (NumberFormatException e) {
                LOG.error("Could not parse the port {}", parts[1]);
            }
        }
    }

    /*public static String buildServersString(List<? extends IConfig> servers) {
        StringBuilder sb = new StringBuilder();
        for (IConfig server : servers) {
            sb.append(server.getString("host"));
            sb.append(':');
            sb.append(server.getInt("port"));
            sb.append(';');
        }
        return servers.isEmpty() ? "" : sb.substring(0, sb.length() - 1);
    }*/
}
