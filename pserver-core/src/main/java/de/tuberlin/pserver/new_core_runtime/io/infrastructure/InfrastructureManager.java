package de.tuberlin.pserver.new_core_runtime.io.infrastructure;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.tuberlin.pserver.new_core_runtime.io.network.NetDescriptor;
import de.tuberlin.pserver.new_core_runtime.io.serializer.KryoFactory;
import de.tuberlin.pserver.new_core_runtime.io.serializer.UnsafeStreamFactory;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZKPaths;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class InfrastructureManager {

    // --------------------------------------------------------------------------------------------------------------
    //
    //  app
    //   |
    //   |- nodes
    //   |   |
    //   |   |- node_0
    //   |   |- node_1
    //   |   |- .
    //   |   |- .
    //   |   |- .
    //   |   |- node_n
    //   |
    //   |- ML-Program
    //   |   |
    //   |   |- state
    //   |   |   |
    //   |   |   |- model_1
    //   |   |   |   |
    //   |   |   |   |- node_0_state_desc
    //   |   |   |   |- node_1_state_desc
    //   |   |   |   |- .
    //   |   |   |   |- .
    //   |   |   |   |- .
    //   |   |   |   |- node_n_state_desc
    //   |   |
    //   |   |- unit
    //   |   |   |
    //   |   |   |- log-reg_1
    //   |   |   |   |
    //   |   |   |   |- unit_barrier
    //   |   |   |   |- node_0_unit_desc
    //   |   |   |   |- node_1_unit_desc
    //   |   |   |   |- .
    //   |   |   |   |- .
    //   |   |   |   |- .
    //   |   |   |   |- node_n_unit_desc
    //
    // --------------------------------------------------------------------------------------------------------------

    public static class GlobalNode<T> {

        public final String name;

        public final T      data;

        public final UUID   remoteRef;

        public final Map<String, GlobalNode<?>> children;

        public GlobalNode(String name, T data) { this(name, data, UUID.randomUUID()); }
        public GlobalNode(String name, T data, UUID remoteRef) {
            this.name       = name;
            this.data       = data;
            this.remoteRef  = remoteRef;
            this.children   = new ConcurrentHashMap<>();
        }

        public String toString() {
            return name;
        }
    }

    // --------------------------------------------------------------------------------------------------------------

    private final class GlobalObjectRepositorySynchronizer {

        public GlobalObjectRepositorySynchronizer() throws Exception{
            TreeCache treeCache = new TreeCache(curator, makePath(appNamespace));
            treeCache.getListenable().addListener((client, event) -> {
                synchronized (zookeeperReadMutex) {
                    try {
                        if (event == null || event.getData() == null)
                            return;

                        String nodePath = event.getData().getPath();
                        String nodeName = ZKPaths.getNodeFromPath(event.getData().getPath());

                        if (nodeName.equals(rootNode.name))
                            return;
                        if (nodeName.equals("globalNodeCounter"))
                            return;

                        String parentNodePath = nodePath.replace(nodeName, "");
                        GlobalNode<?> parentNode = lookup(parentNodePath);

                        switch (event.getType()) {

                            case NODE_ADDED: {
                                System.out.println("TreeNode added: " + event.getData().getPath());
                                GlobalNode<?> newNode = deserializeGlobalNode(event.getData().getData());
                                parentNode.children.put(nodeName, newNode);
                                remoteRefResolver.put(newNode.remoteRef, newNode.data);
                            }
                            break;

                            case NODE_UPDATED: {
                                System.out.println("TreeNode changed: " + event.getData().getPath());
                                GlobalNode<?> updatedNode = deserializeGlobalNode(event.getData().getData());
                                parentNode.children.put(nodeName, updatedNode);
                            }
                            break;

                            case NODE_REMOVED: {
                                System.out.println("TreeNode removed: " + event.getData().getPath());
                                parentNode.children.remove(nodeName);
                            }
                            break;

                            default:
                                System.out.println("Other event: " + event.getType().name());
                        }

                    } catch (Exception ex) {
                        ex.printStackTrace();
                        throw new RuntimeException(ex);
                    }
                }
            });
            treeCache.start();
        }
    }

    // --------------------------------------------------------------------------------------------------------------

    private final CuratorFramework curator;

    private final String appNamespace;

    private final GlobalNode<Void> rootNode;

    private final Kryo kryo;

    private final UnsafeStreamFactory streamFactory;

    private final Object zookeeperReadMutex = new Object();

    private final Map<UUID, Object> remoteRefResolver = new HashMap<>();

    // --------------------------------------------------------------------------------------------------------------

    public InfrastructureManager(String appNamespace,
                                 NetDescriptor netDescriptor,
                                 String connectionString) throws Exception {

        this.appNamespace   = appNamespace;
        this.rootNode       = new GlobalNode<>(appNamespace, null);
        this.curator        = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3));
        this.kryo           = KryoFactory.INSTANCE.create();
        this.streamFactory  = new UnsafeStreamFactory();

        new GlobalObjectRepositorySynchronizer();
        curator.start();
        //addNode(makePath(appNamespace), new GlobalNode<>("nodes_" + getGlobalNodeID(), "STRING"));

        try {

            curator.create().forPath(ZKPaths.PATH_SEPARATOR + "app", serializeGlobalNode(new GlobalNode<>("nodes", new byte[1])));

            curator.create().forPath(ZKPaths.PATH_SEPARATOR + "app" + ZKPaths.PATH_SEPARATOR + "NODES", serializeGlobalNode(new GlobalNode<>("nodes", new byte[1])));


        } catch (Throwable t) {

            t.printStackTrace();
        }

        //addNode(makePath(appNamespace), new GlobalNode<>("node_" + getGlobalNodeID(), netDescriptor));
    }

    public void shutdown() {
        curator.close();
    }


    public <T> void addNode(String path, GlobalNode<T> node) throws Exception {
        curator.create().forPath(path + ZKPaths.PATH_SEPARATOR + node.name, serializeGlobalNode(node));
    }

    public void addDir(String path, String dirName) throws Exception {
        GlobalNode<Void> dirNode = new GlobalNode<>(dirName, null);
        curator.create().forPath(path + ZKPaths.PATH_SEPARATOR + dirNode.name, serializeGlobalNode(dirNode));
    }

    // --------------------------------------------------------------------------------------------------------------

    public static String makePath(String... nodes) {
        StringBuilder sb = new StringBuilder();
        for (String node : nodes)
            sb.append(ZKPaths.PATH_SEPARATOR).append(node);
        return sb.toString();
    }

    private void readZookeeperData() throws Exception {
        synchronized (zookeeperReadMutex) {
            List<String> nodes = curator.getChildren().forPath(makePath(appNamespace));
            for (String node : nodes) {
                byte[] data = curator.getData().forPath(makePath(appNamespace, node));
                GlobalNode<?> newNode = deserializeGlobalNode(data);
                rootNode.children.put(newNode.name, newNode);
            }
        }
    }

    private long getGlobalNodeID() {
        String nodePath = makePath(appNamespace, "globalNodeCounter");
        RetryPolicy retryPolicy = new RetryNTimes(5, 500);
        DistributedAtomicLong globalNodeCounter =
                new DistributedAtomicLong(
                        curator,
                        nodePath,
                        retryPolicy
                );
        long globalNodeID;
        try {
            globalNodeID = globalNodeCounter.increment().postValue();
        } catch (Exception e) {
            throw new IllegalStateException(e.getCause());
        }
        return globalNodeID;
    }

    private byte[] serializeGlobalNode(GlobalNode<?> node) {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        synchronized (kryo) {
            Output output = streamFactory.createOutput(outStream, 1024);
            kryo.writeClassAndObject(output, node.name);
            kryo.writeClassAndObject(output, node.data);
            kryo.writeClassAndObject(output, node.remoteRef);
            output.flush();
        }
        return outStream.toByteArray();
    }

    @SuppressWarnings("unchecked")
    private <T> GlobalNode<T> deserializeGlobalNode(byte[] buffer) {
        GlobalNode<T> node;
        synchronized (kryo) {
            Input input = streamFactory.createInput(new ByteArrayInputStream(buffer), 1024);
            String name    = (String) kryo.readClassAndObject(input);
            Object data    = kryo.readClassAndObject(input);
            UUID remoteRef = (UUID) kryo.readClassAndObject(input);
            node = new GlobalNode<>(name, (T) data, remoteRef);
            input.close();
        }
        return node;
    }

    private GlobalNode<?> lookup(String path) {
        GlobalNode<?> currentNode = rootNode;
        StringTokenizer tokenizer = new StringTokenizer(path, ZKPaths.PATH_SEPARATOR);
        if (!rootNode.name.equals(tokenizer.nextToken()))
            throw new IllegalStateException();
        while (tokenizer.hasMoreTokens()) {
            currentNode = currentNode.children.get(tokenizer.nextToken());
            if (currentNode == null)
                throw new IllegalStateException();
        }
        return currentNode;
    }

    // --------------------------------------------------------------------------------------------------------------

    /*public void start() throws Exception {
        //curator.start();
        boolean isConnected = false;
        try {
            isConnected = curator.blockUntilConnected(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // do nothing.
        }
        if (!isConnected)
            throw new IllegalStateException();

    }

    private CuratorFramework createInstance(String connectionString, String applicationNamespace) {
        int connectionTimeoutMs = 15000;
        int sessionTimeoutMs    = 15000;
        // these are reasonable arguments for the ExponentialBackoffRetry. The first
        // retry will wait 1 second - the second will wait up to 2 seconds - the
        // third will wait up to 4 seconds.
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        // using the CuratorFrameworkFactory.builder() gives fine grained control
        // over creation options. See the CuratorFrameworkFactory.Builder javadoc
        // details
        return CuratorFrameworkFactory.builder()
            .connectString(connectionString)
            .retryPolicy(retryPolicy)
            .connectionTimeoutMs(connectionTimeoutMs)
            .sessionTimeoutMs(sessionTimeoutMs)
            .namespace(applicationNamespace)
            .build();
    }*/
}
