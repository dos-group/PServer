package de.tuberlin.pserver.client;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.core.config.Config;
import de.tuberlin.pserver.runtime.core.events.Event;
import de.tuberlin.pserver.runtime.core.events.IEventHandler;
import de.tuberlin.pserver.runtime.core.infra.InetHelper;
import de.tuberlin.pserver.runtime.core.infra.InfrastructureManager;
import de.tuberlin.pserver.runtime.core.infra.ZookeeperClient;
import de.tuberlin.pserver.runtime.core.network.MachineDescriptor;
import de.tuberlin.pserver.runtime.core.network.NetEvent;
import de.tuberlin.pserver.runtime.core.network.NetManager;
import de.tuberlin.pserver.runtime.core.usercode.UserCodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class PServerClientFactory {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Logger LOG = LoggerFactory.getLogger(PServerClientFactory.class);

    public final Config config;

    public final MachineDescriptor machine;

    public final InfrastructureManager infraManager;

    public final NetManager netManager;

    public final UserCodeManager userCodeManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PServerClientFactory(final Config config) {
        Preconditions.checkNotNull(config);
        final long start = System.nanoTime();
        try {
            final String zookeeperServer = ZookeeperClient.buildServersString(config.getObjectList("global.zookeeper.servers"));
            ZookeeperClient.checkConnectionString(zookeeperServer);
            this.config = Preconditions.checkNotNull(config);
            this.machine = configureMachine();
            this.infraManager = new InfrastructureManager(machine, config, true);
            this.netManager = new NetManager(infraManager, machine, 16);
            this.netManager.start();
            infraManager.start(); // blocking until all at are registered at zookeeper
            infraManager.getMachines().stream().filter(md -> md != machine).forEach(netManager::connect);
            // block until all at are really ready for job submission
            final Set<UUID> responses = new HashSet<>();
            infraManager.getMachines().forEach(md -> responses.add(md.machineID));
            netManager.addEventListener(
                    NetEvent.NetEventTypes.ECHO_RESPONSE,
                    new IEventHandler() {
                        @Override
                        public void handleEvent(Event event) {
                            synchronized (responses) {
                                responses.remove(((NetEvent) event).srcMachineID);
                                responses.notifyAll();
                            }
                        }
                    }
            );
            synchronized (responses) {
                while (!responses.isEmpty()) {
                    try {
                        for (UUID response : responses) {
                            NetEvent event = new NetEvent(NetEvent.NetEventTypes.ECHO_REQUEST);
                            event.setPayload(machine);
                            netManager.dispatchEventAt(response, event);
                        }
                        responses.wait(1000);
                    } catch (InterruptedException e) {
                    }
                }
            }
            this.userCodeManager = new UserCodeManager(this.getClass().getClassLoader());
        } catch(Exception e) {
            throw new IllegalStateException(e);
        }
        LOG.info("PServer Client Startup: " + Long.toString(Math.abs(System.nanoTime() - start) / 1000000) + " ms");
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static PServerClient createPServerClient(Config config) {
        return new PServerClient(new PServerClientFactory(config));
    }

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
