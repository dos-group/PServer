package de.tuberlin.pserver.runtime.core.infra;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.config.Config;
import de.tuberlin.pserver.runtime.core.events.EventDispatcher;
import de.tuberlin.pserver.runtime.core.lifecycle.Deactivatable;
import de.tuberlin.pserver.runtime.core.network.MachineDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class InfrastructureManager extends EventDispatcher implements Deactivatable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(InfrastructureManager.class);

    private final Config config;

    private final MachineDescriptor machine;

    private final Map<UUID, MachineDescriptor> uidDescMap;

    private final List<MachineDescriptor> machines;

    private ZookeeperClient zookeeper;

    public final boolean isClient;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public InfrastructureManager(final MachineDescriptor machine, final Config config, final boolean isClient) {
        super(true, "INFRASTRUCTURE-MANAGER-THREAD");
        this.config     = Preconditions.checkNotNull(config);
        this.machine    = Preconditions.checkNotNull(machine);
        this.uidDescMap = new ConcurrentHashMap<>();
        this.machines   = Collections.synchronizedList(new ArrayList<>());
        this.isClient   = isClient;
        uidDescMap.put(machine.machineID, machine);
    }

    @Override
    public void deactivate() {
        zookeeper.deactivate();
        super.deactivate();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void start() {
        final String zookeeperServer = ZookeeperClient.buildServersString(config.getObjectList("global.zookeeper.servers"));
        ZookeeperClient.checkConnectionString(zookeeperServer);
        try {
            zookeeper = new ZookeeperClient(zookeeperServer);
            zookeeper.initDirectories();
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

    // ---------------------------------------------------

    public Map<UUID, MachineDescriptor> getActivePeers() { return Collections.unmodifiableMap(uidDescMap); }

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

    public MachineDescriptor getMachine(UUID machineID) {
        return uidDescMap.get(machineID);
    }

    // ---------------------------------------------------

    private void loadMachines() {
        int requiredNumNodes = zookeeper.readNumNodes();
        LOG.debug("InfraManager at " + machine.machineID.toString().substring(0,2) + " needs to wait for " + requiredNumNodes + " nodes to register");
        int registeredNumNodes;
        while((registeredNumNodes = readMachinesAndProcess()) < requiredNumNodes) {
            LOG.debug("InfraManager at " + machine.machineID.toString().substring(0,2) + " waits for " + (requiredNumNodes - registeredNumNodes) + " nodes to register");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) { }
        }
        Collections.sort(machines);
    }

    private int readMachinesAndProcess() {
        final List<String> machineList = zookeeper.getChildrenForPath(ZookeeperClient.ZOOKEEPER_NODES);
        for (final String machineIDStr : machineList) {
            final UUID machineID = UUID.fromString(machineIDStr);
            if (!uidDescMap.containsKey(machineID) && !machine.machineID.equals(machineID)) {
                MachineDescriptor md = (MachineDescriptor) zookeeper.readBlocking(ZookeeperClient.ZOOKEEPER_NODES + "/" + machineIDStr);
                uidDescMap.put(machineID, md);
                machines.add(md);
            }
        }
        return machines.size();
    }

}
