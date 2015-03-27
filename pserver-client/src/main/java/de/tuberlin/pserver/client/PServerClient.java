package de.tuberlin.pserver.client;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.PServerJobDescriptor;
import de.tuberlin.pserver.app.UserCodeManager;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.core.infra.InetHelper;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.infra.ZookeeperClient;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class PServerClient extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(PServerClient.class);

    private final NetManager netManager;

    private final UserCodeManager userCodeManager;

    private final List<MachineDescriptor> workers;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PServerClient(final IConfig config) {
        super(true, "PSERVER-CLIENT-THREAD");
        Preconditions.checkNotNull(config);

        final String zookeeperServer = ZookeeperClient.buildServersString(config.getObjectList("zookeeper.servers"));
        ZookeeperClient.checkConnectionString(zookeeperServer);

        MachineDescriptor machine;
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

        final InfrastructureManager infraManager = new InfrastructureManager(machine, config);
        this.netManager = new NetManager(machine, infraManager, 16);
        this.workers = new ArrayList<>();

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

        this.userCodeManager = new UserCodeManager(this.getClass().getClassLoader(), false);
        this.userCodeManager.addStandardDependency("java");
        this.userCodeManager.addStandardDependency("org/apache/log4j");
        this.userCodeManager.addStandardDependency("io/netty");
        this.userCodeManager.addStandardDependency("de/tuberlin/aura/core");
        LOG.info("PServer client is active.");
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void execute(final Class<?> algorithmClass) {
        Preconditions.checkNotNull(algorithmClass);
        final PServerJobDescriptor uc = userCodeManager.extractClass(algorithmClass);
        for (final MachineDescriptor md : workers) {
            final NetEvents.NetEvent event = new NetEvents.NetEvent(PServerJobDescriptor.PSERVER_SUBMIT_JOB_EVENT);
            event.setPayload(uc);
            netManager.sendEvent(md, event);
        }
    }
}
