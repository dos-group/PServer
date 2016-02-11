package de.tuberlin.pserver.node;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.RuntimeContext;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.core.config.IConfig;
import de.tuberlin.pserver.runtime.core.config.IConfigFactory;
import de.tuberlin.pserver.runtime.core.infra.InetHelper;
import de.tuberlin.pserver.runtime.core.infra.InfrastructureManager;
import de.tuberlin.pserver.runtime.core.network.MachineDescriptor;
import de.tuberlin.pserver.runtime.core.network.NetEvent;
import de.tuberlin.pserver.runtime.core.network.NetManager;
import de.tuberlin.pserver.runtime.core.network.RPCManager;
import de.tuberlin.pserver.runtime.core.usercode.UserCodeManager;
import de.tuberlin.pserver.runtime.dht.DHTManager;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.filesystem.FileSystemType;
import de.tuberlin.pserver.runtime.filesystem.hdfs.HDFSFileSystemManagerClient;
import de.tuberlin.pserver.runtime.filesystem.hdfs.HDFSFileSystemManagerServer;
import de.tuberlin.pserver.runtime.filesystem.local.LocalFileSystemManager;
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

    public final IConfig config;

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

    public PServerNodeFactory() { this(IConfigFactory.load(IConfig.Type.PSERVER_NODE)); }

    public PServerNodeFactory(final IConfig config) {

        final long start = System.nanoTime();

        try {

            this.config = Preconditions.checkNotNull(config);
            this.machine = configureMachine();
            this.memoryManager = null; //new MemoryManager(config);
            this.infraManager = new InfrastructureManager(machine, config, false);
            this.netManager = new NetManager(infraManager, machine, 16);
            this.netManager.start();
            this.userCodeManager = new UserCodeManager(this.getClass().getClassLoader());
            this.rpcManager = new RPCManager(netManager);

            infraManager.start(); // blocking until all at are registered at zookeeper
            infraManager.getMachines().stream().filter(md -> md != machine).forEach(netManager::connect);

            this.fileManager = createFileSystem(infraManager.getNodeID());
            this.dhtManager = new DHTManager(this.config, infraManager, netManager);
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
                MachineDescriptor clmd = (MachineDescriptor)((NetEvent) event).getPayload();
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

    public static PServerNode createNode() { return new PServerNode(new PServerNodeFactory()); }

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

    private FileSystemManager createFileSystem(final int nodeID) {
        final String type = config.getString("filesystem.mtxType");
        final FileSystemType fsType = FileSystemType.valueOf(type);
        switch (fsType) {
            case HDFS_FILE_SYSTEM:
                return (config.getInt("filesystem.hdfs.masterNodeIndex") == nodeID)
                        ? new HDFSFileSystemManagerServer(config, infraManager, netManager, rpcManager)
                        : new HDFSFileSystemManagerClient(config, infraManager, netManager, rpcManager);

            case LOCAL_FILE_SYSTEM:
                return new LocalFileSystemManager(infraManager, netManager);
            case NO_FILE_SYSTEM:
                return null;
            default:
                throw new IllegalStateException("No supported filesystem.");
        }
    }
}
