package de.tuberlin.pserver.new_core_runtime.example;


import de.tuberlin.pserver.new_core_runtime.io.infrastructure.InfrastructureManager;
import de.tuberlin.pserver.new_core_runtime.io.infrastructure.NetDescriptorDirectory;
import de.tuberlin.pserver.new_core_runtime.io.network.NetDescriptor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.util.Properties;
import java.util.UUID;

public class InfraTest {

    public static final Logger LOG = LogManager.getRootLogger();

    public static void startupZookeeper() {
        Properties properties = new Properties();
        File file = new File(System.getProperty("java.io.tmpdir")
                + File.separator + UUID.randomUUID());
        file.deleteOnExit();
        properties.setProperty("dataDir", file.getAbsolutePath());
        properties.setProperty("clientPort", String.valueOf(2181));
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        try {
            quorumConfiguration.parseProperties(properties);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        ZooKeeperServerMain zooKeeperServer = new ZooKeeperServerMain();
        ServerConfig configuration = new ServerConfig();
        configuration.readFrom(quorumConfiguration);
        new Thread() {
            public void run() {
                try {
                    zooKeeperServer.runFromConfig(configuration);
                } catch (Exception e) {
                    LOG.error("ZooKeeper Failed", e);
                }
            }
        }.start();
    }

    public static void main(String[] args) throws Exception {
        startupZookeeper();
        try {
            NetDescriptor nd = NetDescriptorDirectory.createRandomLocalhostNetDescriptor();
            InfrastructureManager im = new InfrastructureManager("app", nd, "localhost:2181");

            //im.addDir("/app", "nodes");
            //im.addDir("/app", "state");
            //im.addDir("/app", "unit");

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
