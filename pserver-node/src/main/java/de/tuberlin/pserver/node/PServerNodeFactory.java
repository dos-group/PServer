package de.tuberlin.pserver.node;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.diagnostics.MemoryTracer;
import de.tuberlin.pserver.runtime.RuntimeContext;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.commons.config.Config;
import de.tuberlin.pserver.runtime.core.infra.InetHelper;
import de.tuberlin.pserver.runtime.core.infra.InfrastructureManager;
import de.tuberlin.pserver.runtime.core.network.MachineDescriptor;
import de.tuberlin.pserver.runtime.core.network.NetEvent;
import de.tuberlin.pserver.runtime.core.network.NetManager;
import de.tuberlin.pserver.runtime.core.network.RPCManager;
import de.tuberlin.pserver.runtime.core.usercode.UserCodeManager;
import de.tuberlin.pserver.runtime.dht.DHTManager;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.memory.MemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.UUID;

public final class PServerNodeFactory {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Logger LOG = LoggerFactory.getLogger(PServerNodeFactory.class);

    public final Config config;

    public final MachineDescriptor machine;

    public final MemoryManager memoryManager;

    public final InfrastructureManager infraManager;

    public final NetManager netManager;

    public final FileSystemManager fileManager;

    public final UserCodeManager userCodeManager;

    public final DHTManager dhtManager;

    public final RuntimeManager runtimeManager;

    public final RPCManager rpcManager;

    public final RuntimeContext runtimeContext;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PServerNodeFactory(final Config config) {
        final long start = System.nanoTime();
        try {
            this.config = Preconditions.checkNotNull(config);
            this.machine = configureMachine();
            this.memoryManager = null; //new MemoryManager(config);
            this.infraManager = new InfrastructureManager(machine, config, false);

            MemoryTracer.printTrace("Initialized_InfraStructureManager");
            this.netManager = new NetManager(infraManager, machine, 16);
            this.netManager.start();

            MemoryTracer.printTrace("Initialized_NetManager");
            this.userCodeManager = new UserCodeManager(this.getClass().getClassLoader());
            this.rpcManager = new RPCManager(netManager);
            infraManager.start(); // blocking until all at are registered at zookeeper

            Thread.sleep(infraManager.getNodeID() * 500); // TODO: AVOID DOUBLE CONNECTIONS...

            infraManager.getMachines().stream().filter(md -> md != machine).forEach(netManager::connect);
            System.out.println("NUM OF ACTIVE CHANNELS: " + netManager.getActiveChannels().size() + " | nodeID = " + infraManager.getNodeID());

            MemoryTracer.printTrace("Connected_RemoteMachines");
            FileSystemManager.FileSystemType type = FileSystemManager.FileSystemType.valueOf(config.getString("worker.filesystem.type"));
            this.fileManager = new FileSystemManager(config, infraManager, netManager, type, infraManager.getNodeID());

            MemoryTracer.printTrace("Initialized_FileSystemManager");
            this.dhtManager = new DHTManager(this.config, infraManager, netManager);

            MemoryTracer.printTrace("Initialized_DHTManager");
            this.runtimeManager = new RuntimeManager(infraManager, netManager, fileManager, dhtManager);
            this.runtimeContext = new RuntimeContext(
                    machine,
                    infraManager.getMachines().size(),
                    Runtime.getRuntime().availableProcessors(),
                    infraManager.getNodeID(),
                    netManager,
                    dhtManager,
                    fileManager,
                    runtimeManager,
                    infraManager
            );
            netManager.addEventListener(NetEvent.NetEventTypes.ECHO_REQUEST, event -> {
                MachineDescriptor clmd = (MachineDescriptor) event.getPayload();
                netManager.dispatchEventAt(clmd, new NetEvent(NetEvent.NetEventTypes.ECHO_RESPONSE));
            });
        } catch(Exception e) {
            throw new IllegalStateException(e);
        }
        LOG.info("PServer Node Startup: " + Long.toString(Math.abs(System.nanoTime() - start) / 1000000) + " ms");
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static PServerNode createNode(Config config) { return new PServerNode(new PServerNodeFactory(config)); }

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
        LOG.debug("Starting PServer Node with id : " + machine.machineID.toString().substring(0,2));
        return machine;
    }
}
