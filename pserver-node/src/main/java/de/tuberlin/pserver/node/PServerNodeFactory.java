package de.tuberlin.pserver.node;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.config.IConfigFactory;
import de.tuberlin.pserver.core.infra.InetHelper;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.core.net.RPCManager;
import de.tuberlin.pserver.runtime.RuntimeContext;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.dht.DHTManager;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.filesystem.FileSystemType;
import de.tuberlin.pserver.runtime.filesystem.hdfs.HDFSFileSystemManagerClient;
import de.tuberlin.pserver.runtime.filesystem.hdfs.HDFSFileSystemManagerServer;
import de.tuberlin.pserver.runtime.filesystem.local.LocalFileSystemManager;
import de.tuberlin.pserver.runtime.memory.MemoryManager;
import de.tuberlin.pserver.runtime.usercode.UserCodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.UUID;

public enum PServerNodeFactory {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    INSTANCE(IConfigFactory.load(IConfig.Type.PSERVER_NODE));

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

    private PServerNodeFactory(final IConfig config) {

        final long start = System.nanoTime();

        this.config             = Preconditions.checkNotNull(config);
        this.machine            = configureMachine();
        this.memoryManager      = null; //new MemoryManager(config);
        this.infraManager       = new InfrastructureManager(machine, config, false);
        this.netManager         = new NetManager(machine, infraManager, 16);
        this.userCodeManager    = new UserCodeManager(this.getClass().getClassLoader());
        this.rpcManager         = new RPCManager(netManager);

        infraManager.start(); // blocking until all at are registered at zookeeper
        infraManager.getMachines().stream().filter(md -> md != machine).forEach(netManager::connectTo);

        this.fileManager        = createFileSystem(infraManager.getNodeID());
        this.dhtManager         = new DHTManager(this.config, infraManager, netManager);
        this.runtimeManager     = new RuntimeManager(infraManager, netManager, fileManager, dhtManager);

        this.runtimeContext = new RuntimeContext(
                machine,
                infraManager.getMachines().size(),
                Runtime.getRuntime().availableProcessors(),
                infraManager.getNodeID(),
                netManager,
                DHTManager.getInstance(),
                runtimeManager
        );

        netManager.addEventListener(NetEvents.NetEventTypes.ECHO_REQUEST, event -> {
            UUID dst = ((NetEvents.NetEvent) event).srcMachineID;
            netManager.sendEvent(dst, new NetEvents.NetEvent(NetEvents.NetEventTypes.ECHO_RESPONSE));
        });

        LOG.info("PServer Node Startup: " + Long.toString(Math.abs(System.nanoTime() - start) / 1000000) + " ms");
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static PServerNode createParameterServerNode() { return new PServerNode(INSTANCE); }

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
        final String type = config.getString("filesystem.type");
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
