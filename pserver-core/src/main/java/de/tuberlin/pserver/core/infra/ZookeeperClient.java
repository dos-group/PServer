package de.tuberlin.pserver.core.infra;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.config.IConfig;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class ZookeeperClient {

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

    public static int CONNECTION_TIMEOUT = 1000;

    public static int CONNECTION_RETRIES = 15;

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
        curator = CuratorFrameworkFactory.newClient(zookeeperServer, CONNECTION_TIMEOUT, CONNECTION_TIMEOUT, new RetryNTimes(CONNECTION_RETRIES, CONNECTION_TIMEOUT));
        curator.start();
        CuratorZookeeperClient client = curator.getZookeeperClient();
        int tries = 1;
        while(tries <= CONNECTION_RETRIES && !client.isConnected()) {
            LOG.debug("Unsuccessful try ("+tries+") to connect to zookeeper at " + client.getCurrentConnectionString());
            try {
                Thread.sleep(CONNECTION_TIMEOUT);
            } catch (InterruptedException e) {}
            tries++;
        }
        if(!client.isConnected()) {
            LOG.error("Unable to connect to zookeeper at " + client.getCurrentConnectionString());
            System.exit(1);
        }
        LOG.info("Successfully connected to zookeeper at " + client.getCurrentConnectionString());
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
                // These at only need to exist. No matter who created them.
            }
        }
        stat = curator.checkExists().forPath(ZOOKEEPER_NODES);
        if (stat == null) {
            try {
                curator.create().forPath(ZOOKEEPER_NODES, new byte[0]);
            } catch (KeeperException.NodeExistsException e) {
                // These at only need to exist. No matter who created them.
            }
        }
    }

    public void create(final String path) {
        try {
            Stat stat = curator.checkExists().forPath(path);
            if (stat == null) {
                try {
                    curator.create().forPath(path, new byte[0]);
                } catch (KeeperException.NodeExistsException e) {
                    // These at only need to exist. No matter who created them.
                }
            }
        } catch (Exception e) {
            //throw new IllegalStateException(e);
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

    public void writeNumNodes(int numNodes) throws Exception {
        try {
            String numNodesStr = String.valueOf(numNodes);
            byte[] bytes = new byte[numNodesStr.length()];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) numNodesStr.charAt(i);
            }
            curator.create().forPath("/numnodes", bytes);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public int readNumNodes() {
        try {
            final byte[] data = readByteArrayBlocking("/numnodes");
            int numnodes = 0;
            for (int i = data.length - 1; i >= 0; i--) {
                numnodes += Integer.valueOf(String.valueOf((char)data[i])) * (int)Math.pow(10, data.length - (i + 1));
            }
            return numnodes;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public byte[] readByteArrayBlocking(final String path) {
        byte[] result = null;
        boolean readSuccess;
        do {
            try {
                result = curator.getData().forPath(path);
                readSuccess = true;
            } catch (Exception e) {
                LOG.debug("failed to read path: " + path, e);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {}
                readSuccess = false;
            }
        } while (!readSuccess);
        return result;
    }

    public Object readBlocking(final String path) {
        Object result = null;
        boolean readSuccess;
        do {
            try {
                result = read(path);
                readSuccess = true;
            } catch (Exception e) {
                LOG.debug("failed to read path: " + path, e);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {}
                readSuccess = false;
            }
        } while (!readSuccess);
        return result;
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

    public static String buildServersString(List<? extends IConfig> servers) {
        StringBuilder sb = new StringBuilder();
        for (final IConfig server : servers) {
            sb.append(server.getString("host"));
            sb.append(':');
            sb.append(server.getInt("port"));
            sb.append(';');
        }
        return servers.isEmpty() ? "" : sb.substring(0, sb.length() - 1);
    }

    // TODO: unit test?
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        // write
        String numNodesStr = String.valueOf("8");
        byte[] bytes = new byte[numNodesStr.length()];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) numNodesStr.charAt(i);
        }
        System.out.println(Arrays.toString(bytes));
        // read
        final byte[] data = bytes;
        int numnodes = 0;
        for (int i = data.length - 1; i >= 0; i--) {
            numnodes += Integer.valueOf(String.valueOf((char)data[i])) * (int)Math.pow(10, data.length - (i + 1));
        }
        System.out.println(numnodes);
    }

}
