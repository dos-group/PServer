package de.tuberlin.pserver.runtime.core.infra;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.core.common.Deactivatable;
import de.tuberlin.pserver.runtime.core.config.IConfig;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class ClusterSimulator implements Deactivatable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(ClusterSimulator.class);

    private final List<ProcessExecutor> peList;

    private final ZooKeeperServer zookeeperServer;

    private final NIOServerCnxnFactory zookeeperCNXNFactory;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ClusterSimulator(final IConfig config,
                            final boolean isDebug,
                            final Class<?> mainClass) {

        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(mainClass);

        final int numNodes = config.getInt("simulation.numNodes");

        final int tickTime = 1;
        final int numConnections = 50;
        final String zkServer = ZookeeperClient.buildServersString(config.getObjectList("zookeeper.servers"));
        final int zkPort = config.getObjectList("zookeeper.servers").get(0).getInt("port");
        final boolean useZookeeper = config.getBoolean("simulation.useZookeeper");
        final List<String> jvmOpts = config.getStringList("simulation.jvmOptions");

        // sanity check.
        ZookeeperClient.checkConnectionString(zkServer);

        if (numNodes < 1)
            throw new IllegalArgumentException("numNodes < 1");

        this.peList = new ArrayList<>();

        // ------- bootstrap zookeeper server -------

        if (useZookeeper) {
            final File dir = new File(System.getProperty("java.io.tmpdir"), "zookeeper").getAbsoluteFile();
            if (dir.exists()) {
                try {
                    FileUtils.deleteDirectory(dir);
                } catch (IOException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                }
            }
            try {
                this.zookeeperServer = new ZooKeeperServer(dir, dir, tickTime);
                this.zookeeperServer.setMaxSessionTimeout(10000000);
                this.zookeeperServer.setMinSessionTimeout(10000000);
                this.zookeeperCNXNFactory = new NIOServerCnxnFactory();
                this.zookeeperCNXNFactory.configure(new InetSocketAddress(zkPort), numConnections);
                this.zookeeperCNXNFactory.startup(zookeeperServer);
            } catch (IOException | InterruptedException e) {
                throw new IllegalStateException(e);
            }

            // Write number of stateObjectNodes to zookeeper.
            ZookeeperClient zookeeperClient = new ZookeeperClient(zkServer);
            try {
                zookeeperClient.writeNumNodes(numNodes);
            } catch (Exception e) {
                e.printStackTrace();
            }
            zookeeperClient.deactivate();

        } else {
            zookeeperServer = null;
            zookeeperCNXNFactory = null;
        }

        // ------- bootstrap cluster -------

        if (!isDebug) {
            for (int i = 0; i < numNodes; ++i) {
                //List<String> nodeJvmOpts = new ArrayList<>(jvmOpts);
                // nodeJvmOpts.add(String.format("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=%d",8000+i));
                peList.add(new ProcessExecutor(mainClass).execute(jvmOpts.toArray(new String[jvmOpts.size()])));
            }
        }
    }

    @Override
    public void deactivate() {
        peList.forEach(ClusterSimulator.ProcessExecutor::destroy);
        this.zookeeperServer.shutdown();
        this.zookeeperCNXNFactory.closeAll();
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static final class ProcessExecutor {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private final Class<?> executableClazz;

        private Process process;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public ProcessExecutor(final Class<?> executableClazz) {
            Preconditions.checkNotNull(executableClazz);
            try {
                executableClazz.getMethod("main", String[].class);
            } catch (NoSuchMethodException | SecurityException e) {
                throw new IllegalArgumentException(e);
            }
            this.executableClazz = executableClazz;
            this.process = null;
        }

        // ---------------------------------------------------
        // Public.
        // ---------------------------------------------------

        public ProcessExecutor execute(String[] jvmOpts, String... params) {
            final String javaRuntime = System.getProperty("java.home") + "/bin/java";
            final String classpath =
                    System.getProperty("java.class.path") + ":"
                            + executableClazz.getProtectionDomain().getCodeSource().getLocation().getPath();
            final String canonicalName = executableClazz.getCanonicalName();
            try {
                final List<String> commandList = new ArrayList<>();
                commandList.add(javaRuntime);
                commandList.add("-cp");
                commandList.add(classpath);
                commandList.addAll(Arrays.asList(jvmOpts));
                commandList.add(canonicalName);
                commandList.addAll(Arrays.asList(params));
                final ProcessBuilder builder = new ProcessBuilder(commandList);
                builder.redirectErrorStream(true);
                builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
                process = builder.start();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return this;
        }

        public void destroy() {
            Preconditions.checkNotNull(process);
            process.destroy();
        }
    }
}
