package de.tuberlin.pserver.core.infra;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.events.EventDispatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class InfrastructureManager extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(InfrastructureManager.class);

    private final IConfig config;

    private final MachineDescriptor machine;

    private final Map<UUID, MachineDescriptor> peers;

    private final List<MachineDescriptor> machines;

    private ZookeeperClient zookeeper;

    public final boolean isClient;


    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public InfrastructureManager(final MachineDescriptor machine, final IConfig config, final boolean isClient) {
        super(true, "INFRASTRUCTURE-MANAGER-THREAD");
        this.config     = Preconditions.checkNotNull(config);
        this.machine    = Preconditions.checkNotNull(machine);
        this.peers      = new ConcurrentHashMap<>();
        this.machines   = Collections.synchronizedList(new ArrayList<>());
        this.isClient   = isClient;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void start() {
        final String zookeeperServer = ZookeeperClient.buildServersString(config.getObjectList("zookeeper.servers"));
        ZookeeperClient.checkConnectionString(zookeeperServer);
        try {
            zookeeper = new ZookeeperClient(zookeeperServer);
            zookeeper.initDirectories();
            Thread.sleep(1000);
            if (!isClient) {
                zookeeper.store(ZookeeperClient.ZOOKEEPER_NODES + "/" + machine.machineID.toString(), machine);
                machines.add(machine);
            }
            loadMachines();
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
        LOG.debug("Started InfrastructureManager at " + machine);
    }

    private final Object lock = new Object();

    public Map<UUID, MachineDescriptor> getActivePeers() { return Collections.unmodifiableMap(peers); }

    public MachineDescriptor getMachine() { return Preconditions.checkNotNull(machine); }

    public List<MachineDescriptor> getMachines() { return Collections.unmodifiableList(machines); }

    public int getNodeID() { return machines.indexOf(machine); }

    public int getMachineIndex(final MachineDescriptor machine) { return machines.indexOf(Preconditions.checkNotNull(machine)); }

    public int getNodeIDFromMachineUID(final UUID machineUID) {
        Preconditions.checkNotNull(machineUID);
        for (int i = 0; i < machines.size(); ++i) {
            if (machines.get(i).machineID.equals(machineUID))
                return i;
        }
        throw new IllegalStateException();
    }

    public MachineDescriptor getMachine(final int machineIndex) {
        if (machineIndex >= machines.size())
            return null;
        return machines.get(machineIndex);
    }

    // ---------------------------------------------------

    @Override
    public void deactivate() { super.deactivate(); }

    private void loadMachines() {
        int requiredNumNodes = Integer.parseInt((String)zookeeper.readBlocking("/numnodes"));
        if(!isClient) {
            requiredNumNodes--;
        }
        while(readMachinesAndProcess() < requiredNumNodes) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        Collections.sort(machines);
    }

    private int readMachinesAndProcess() {
        final List<String> machineList = zookeeper.getChildrenForPath(ZookeeperClient.ZOOKEEPER_NODES);
        for (final String machineIDStr : machineList) {
            final UUID machineID = UUID.fromString(machineIDStr);
            if (!peers.containsKey(machineID) && !machine.machineID.equals(machineID)) {
                MachineDescriptor md = (MachineDescriptor) zookeeper.readBlocking(ZookeeperClient.ZOOKEEPER_NODES + "/" + machineIDStr);
                //System.out.println("["+machine.machineID.toString().substring(0,4) + "]["+isClient+"]: added "+md.machineID.toString().substring(0,4));
                peers.put(machineID, md);
                machines.add(md);
            }
        }
        return machines.size();
    }

}
