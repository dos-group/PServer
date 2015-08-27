package de.tuberlin.pserver.core.infra;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.events.Event;
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

    private final Object lock = new Object();

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public InfrastructureManager(final MachineDescriptor machine, final IConfig config) {
        super(true, "INFRASTRUCTURE-MANAGER-THREAD");
        this.config     = Preconditions.checkNotNull(config);
        this.machine    = Preconditions.checkNotNull(machine);
        this.peers      = new ConcurrentHashMap<>();
        this.machines   = Collections.synchronizedList(new ArrayList<>());
        machines.add(machine);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void start() {
        final String zookeeperServer = ZookeeperClient.buildServersString(config.getObjectList("zookeeper.servers"));
        ZookeeperClient.checkConnectionString(zookeeperServer);
        connectZookeeper(zookeeperServer);
        LOG.debug("Started InfrastructureManager at " + machine);
    }

    public int getNodeIDFromMachineUID(final UUID machineUID) {
        Preconditions.checkNotNull(machineUID);
        for (int i = 0; i < machines.size(); ++i) {
            if (machines.get(i).machineID.equals(machineUID))
                return i;
        }
        throw new IllegalStateException();
    }


    public Map<UUID, MachineDescriptor> getActivePeers() { return Collections.unmodifiableMap(peers); }

    public MachineDescriptor getMachine() { return Preconditions.checkNotNull(machine); }

    public List<MachineDescriptor> getMachines() { return Collections.unmodifiableList(machines); }

    public int getNodeID() { return machines.indexOf(machine); }

    public int getMachineIndex(final MachineDescriptor machine) { return machines.indexOf(Preconditions.checkNotNull(machine)); }

    public MachineDescriptor getMachine(final int machineIndex) {
        if (machineIndex >= machines.size())
            return null;
        return machines.get(machineIndex);
    }

    public int getNumOfNodesFromZookeeper() {
        String numNodesStr = null;
        try {
            numNodesStr = (String)zookeeper.read("/numnodes");
        } catch (Exception e) {
            //throw new IllegalStateException(e);
        }
        return (numNodesStr == null) ? -1 : Integer.parseInt(numNodesStr);
    }

    // ---------------------------------------------------

    @Override
    public void deactivate() { super.deactivate(); }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void connectZookeeper(final String server) {
        Preconditions.checkNotNull(server);
        try {
            ZookeeperClient zookeeper = new ZookeeperClient(server);
            zookeeper.initDirectories();
            final String zkNodeDir = ZookeeperClient.ZOOKEEPER_NODES + "/" + machine.machineID.toString();
            zookeeper.store(zkNodeDir, machine);
            final InfrastructureWatcher watcher = new InfrastructureWatcher();
            final List<String> machineIDs = zookeeper.getChildrenForPathAndWatch(ZookeeperClient.ZOOKEEPER_NODES, watcher);
            synchronized (lock) {
                for (final String machineID : machineIDs) {
                    final MachineDescriptor md = (MachineDescriptor) zookeeper.read(ZookeeperClient.ZOOKEEPER_NODES + "/" + machineID);
                    if (!md.machineID.equals(machine.machineID)) {
                        peers.put(md.machineID, md);
                        machines.add(md);
                        Collections.sort(machines);
                        dispatchEvent(new Event(ZookeeperClient.IM_EVENT_NODE_ADDED, md));
                    }
                }
            }
            this.zookeeper = zookeeper;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private class InfrastructureWatcher implements Watcher {

        @Override
        public synchronized void process(final WatchedEvent event) {
            //try {
                synchronized (lock) {

                    while (zookeeper == null) // Killer solution ! :)
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                    zookeeper.getChildrenForPathAndWatch(ZookeeperClient.ZOOKEEPER_NODES, this);
                    final List<String> machineList = zookeeper.getChildrenForPath(ZookeeperClient.ZOOKEEPER_NODES);
                    // Find out whether a node was created or deleted.
                    //if (peers.values().length() < machineList.length()) {
                        // A node has been added.
                        MachineDescriptor md = null;
                        for (final String machineIDStr : machineList) {
                            final UUID machineID = UUID.fromString(machineIDStr);
                            if (!peers.containsKey(machineID)) {

                                boolean readSuccess;
                                do {
                                    try {
                                        md = (MachineDescriptor) zookeeper.read(event.getPath() + "/" + machineIDStr);
                                        readSuccess = true;
                                    } catch (Exception e) {
                                        readSuccess = false;
                                    }
                                } while (!readSuccess);

                                if (!md.machineID.equals(machine.machineID)) {
                                    peers.put(machineID, md);
                                    machines.add(md);
                                    Collections.sort(machines);
                                    dispatchEvent(new de.tuberlin.pserver.core.events.Event(ZookeeperClient.IM_EVENT_NODE_ADDED, md));
                                }
                            }
                        }
                    /*} else {
                        // A node has been removed.
                        UUID machineID = null;
                        for (final UUID uid : peers.keySet()) {
                            if (!machineList.contains(uid.toString())) {
                                machineID = uid;
                                break;
                            }
                        }
                        final MachineDescriptor removedMachine = peers.remove(machineID);
                        machines.remove(removedMachine);
                        Collections.sort(machines);
                        dispatchEvent(new de.tuberlin.pserver.core.events.Event(ZookeeperClient.IM_EVENT_NODE_REMOVED, removedMachine));
                    }*/

                    // keep watching
                }
            //} catch (Exception e) {
            //    LOG.error(e.getLocalizedMessage());
            //}
        }
    }
}
