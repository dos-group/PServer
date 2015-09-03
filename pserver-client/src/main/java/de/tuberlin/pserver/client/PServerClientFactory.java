package de.tuberlin.pserver.client;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.config.IConfigFactory;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InetHelper;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.infra.ZookeeperClient;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.runtime.usercode.UserCodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
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


        this.infraManager   = new InfrastructureManager(machine, config, true);
        this.netManager     = new NetManager(machine, infraManager, 16);

        infraManager.start(); // blocking until all nodes are registered at zookeeper
        infraManager.getMachines().stream().filter(md -> md != machine).forEach(netManager::connectTo);

        // block until all nodes are really ready for job submission
        final Set<UUID> responses = new HashSet<>();
        infraManager.getMachines().forEach(md -> responses.add(md.machineID));
        netManager.addEventListener(
                NetEvents.NetEventTypes.ECHO_RESPONSE,
                new IEventHandler() {
                    @Override
                    public void handleEvent(Event event) {
                        synchronized (responses) {
                            responses.remove(((NetEvents.NetEvent) event).srcMachineID);
                            responses.notifyAll();
                        }
                    }
                }
        );
        //THE ABOVE DOES NOT WORK WITH LAMBDAS !!!
        //EXCEPT WITH REFERENCE TO "this"
        //???
        //netManager.addEventListener(
        //      NetEvents.NetEventTypes.ECHO_RESPONSE,
        //      event -> {
        //            //String mandatoryFooBarReferenceToParentObject = machine.machineID.toString();
        //            synchronized (responses) {
        //                responses.remove(((NetEvents.NetEvent) event).srcMachineID);
        //                responses.notifyAll();
        //            }
        //});
        synchronized (responses) {
            while(!responses.isEmpty()) {
                try {
                    for (UUID response : responses) {
                        NetEvents.NetEvent event = new NetEvents.NetEvent(NetEvents.NetEventTypes.ECHO_REQUEST);
                        event.setPayload(machine);
                        netManager.sendEvent(response, event);
                    }
                    responses.wait(1000);
                } catch (InterruptedException e) {}
            }
        }

        this.userCodeManager = new UserCodeManager(this.getClass().getClassLoader());
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
