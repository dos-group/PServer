package de.tuberlin.pserver.node;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.UserCodeManager;
import de.tuberlin.pserver.app.dht.DHT;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.config.IConfigFactory;
import de.tuberlin.pserver.core.filesystem.hdfs.HDFSManager;
import de.tuberlin.pserver.core.infra.InetHelper;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.infra.ZookeeperClient;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.core.net.RPCManager;
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

    public final InfrastructureManager infraManager;

    public final NetManager netManager;

    public final HDFSManager hdfsManager;

    public final UserCodeManager userCodeManager;

    public final DataManager dataManager;

    public final RPCManager rpcManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    private PServerNodeFactory(final IConfig config) {

        final long start = System.nanoTime();

        this.config             = Preconditions.checkNotNull(config);
        this.machine            = configureMachine();
        this.infraManager       = new InfrastructureManager(machine, config);
        this.netManager         = new NetManager(machine, infraManager, 16);
        this.userCodeManager    = new UserCodeManager(this.getClass().getClassLoader(), false);
        this.rpcManager         = new RPCManager(netManager);

        infraManager.addEventListener(ZookeeperClient.IM_EVENT_NODE_ADDED, event -> {
            if (event.getPayload() instanceof MachineDescriptor) {
                final MachineDescriptor md = (MachineDescriptor)event.getPayload();
                netManager.connectTo(md);
            } else
                throw new IllegalStateException();
        });

        infraManager.start();

        try {
            Thread.sleep(2000);
        } catch (Exception e) {
            throw new IllegalStateException();
        }

        if (this.config.getBoolean("filesystem.hdfs.use")
                && this.config.getInt("filesystem.hdfs.masterNodeIndex") == 0)
            hdfsManager = new HDFSManager(this.config, infraManager, netManager, rpcManager);
        else
            hdfsManager = null;

        DHT.getInstance().initialize(infraManager, netManager);
        this.dataManager = new DataManager(this.config, infraManager, netManager, rpcManager, hdfsManager, DHT.getInstance());

        LOG.info(infraManager.getMachine()
                + " | " + infraManager.getCurrentMachineIndex()
                + " | " + infraManager.getActivePeers().size()
                + " | " + infraManager.getMachines().size());

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
}
