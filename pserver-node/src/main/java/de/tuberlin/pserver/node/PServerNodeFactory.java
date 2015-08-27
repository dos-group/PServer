package de.tuberlin.pserver.node;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.config.IConfigFactory;
import de.tuberlin.pserver.core.infra.InetHelper;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.infra.ZookeeperClient;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.core.net.RPCManager;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.ExecutionManager;
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
import java.util.concurrent.atomic.AtomicInteger;

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

    public final FileSystemManager fileSystemManager;

    public final UserCodeManager userCodeManager;

    public final DHTManager dht;

    public final DataManager dataManager;

    public final RPCManager rpcManager;

    public final ExecutionManager executionManager;

    private final AtomicInteger detectedNodeNum = new AtomicInteger(0);

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    private PServerNodeFactory(final IConfig config) {

        final long start = System.nanoTime();

        this.config             = Preconditions.checkNotNull(config);
        this.machine            = configureMachine();
        this.memoryManager      = null; //new MemoryManager(config);
        this.infraManager       = new InfrastructureManager(machine, config);
        this.netManager         = new NetManager(machine, infraManager, 16);
        this.userCodeManager    = new UserCodeManager(this.getClass().getClassLoader());
        this.rpcManager         = new RPCManager(netManager);

        infraManager.addEventListener(ZookeeperClient.IM_EVENT_NODE_ADDED, event -> {
            if (event.getPayload() instanceof MachineDescriptor) {
                final MachineDescriptor md = (MachineDescriptor) event.getPayload();
                if (!machine.machineID.equals(md.machineID)) {
                    netManager.connectTo(md);
                    detectedNodeNum.incrementAndGet();
                }
            } else
                throw new IllegalStateException();
        });

        infraManager.start();

        // Active waiting until all nodes are available!
        while (infraManager.getNumOfNodesFromZookeeper() - 1 != detectedNodeNum.get()) {
            try { Thread.sleep(1000); } catch(Exception e) { LOG.error(e.getMessage()); }
        }

        this.fileSystemManager  = createFileSystem(infraManager.getNodeID());
        this.dht                = new DHTManager(this.config, infraManager, netManager);
        this.executionManager   = new ExecutionManager(Runtime.getRuntime().availableProcessors(), netManager);
        this.dataManager        = new DataManager(this.config, infraManager, netManager, executionManager, fileSystemManager, dht);

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
