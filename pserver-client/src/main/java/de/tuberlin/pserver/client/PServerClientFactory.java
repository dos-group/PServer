package de.tuberlin.pserver.client;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.config.IConfigFactory;
import de.tuberlin.pserver.core.infra.InetHelper;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.infra.ZookeeperClient;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.runtime.usercode.UserCodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public enum PServerClientFactory {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    INSTANCE(IConfigFactory.load(IConfig.Type.PSERVER_CLIENT));

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Logger LOG = LoggerFactory.getLogger(PServerClientFactory.class);

    public final IConfig config;

    public final MachineDescriptor machine;

    public final InfrastructureManager infraManager;

    public final NetManager netManager;

    public final UserCodeManager userCodeManager;

    public final List<MachineDescriptor> workers;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    private PServerClientFactory(final IConfig config) {
        Preconditions.checkNotNull(config);

        final long start = System.nanoTime();

        final String zookeeperServer = ZookeeperClient.buildServersString(config.getObjectList("zookeeper.servers"));
        ZookeeperClient.checkConnectionString(zookeeperServer);

        this.config         = Preconditions.checkNotNull(config);
        this.machine        = configureMachine();
        this.workers        = new ArrayList<>();
        this.infraManager   = new InfrastructureManager(machine, config);
        this.netManager     = new NetManager(machine, infraManager, 16);

        try {
            ZookeeperClient zookeeper = new ZookeeperClient(zookeeperServer);
            final List<String> machineIDs = zookeeper.getChildrenForPath(ZookeeperClient.ZOOKEEPER_NODES);
            for (final String machineID : machineIDs) {
                final MachineDescriptor md = (MachineDescriptor) zookeeper.read(ZookeeperClient.ZOOKEEPER_NODES + "/" + machineID);
                netManager.connectTo(md);
                workers.add(md);
            }
            zookeeper.close();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        this.userCodeManager = new UserCodeManager(this.getClass().getClassLoader());
        /*this.userCodeManager.addStandardDependency("java");
        this.userCodeManager.addStandardDependency("org/apache/log4j");
        this.userCodeManager.addStandardDependency("io/netty");
        this.userCodeManager.addStandardDependency("de/tuberlin/aura/core");*/

        LOG.info("PServer Client Startup: " + Long.toString(Math.abs(System.nanoTime() - start) / 1000000) + " ms");
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static PServerClient createPServerClient() { return new PServerClient(INSTANCE); }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private MachineDescriptor configureMachine() {
        final MachineDescriptor machine;
        try {
            machine = new MachineDescriptor(
                    UUID.randomUUID(),
                    InetHelper.getIPAddress(),
                    InetHelper.getFreePort(),
                    InetAddress.getLocalHost().getHostName()
            );
        } catch(Throwable t) {
            throw new IllegalStateException(t);
        }
        return machine;
    }
}
