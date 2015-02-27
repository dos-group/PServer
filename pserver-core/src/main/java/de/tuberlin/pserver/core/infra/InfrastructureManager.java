package de.tuberlin.pserver.core.infra;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.EventDispatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class InfrastructureManager extends EventDispatcher {

    public static final String ZOOKEEPER_SERVER = "localhost:2181";

    //public static final String ZOOKEEPER_SERVER = "130.149.249.11:2181";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(InfrastructureManager.class);

    private final MachineDescriptor machine;

    private final Map<UUID, MachineDescriptor> peers;

    private final List<MachineDescriptor> machines;

    private ZookeeperClient zookeeper;

    private final Object lock = new Object();

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public InfrastructureManager(final MachineDescriptor machine) {
        super(true, "INFRASTRUCTURE-MANAGER-THREAD");
        this.machine = Preconditions.checkNotNull(machine);
        this.peers = new ConcurrentHashMap<>();
        this.machines = Collections.synchronizedList(new ArrayList<MachineDescriptor>());
        machines.add(machine);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void start() {
        connectZookeeper(ZOOKEEPER_SERVER);
    }

    public Map<UUID, MachineDescriptor> getActivePeers() {
        return Collections.unmodifiableMap(peers);
    }

    public MachineDescriptor getMachine() { return Preconditions.checkNotNull(machine); }

    public List<MachineDescriptor> getMachines() { return Collections.unmodifiableList(machines); }

    public int getCurrentMachineIndex() { return machines.indexOf(machine); }

    public int getMachineIndex(final MachineDescriptor machine) { return machines.indexOf(Preconditions.checkNotNull(machine)); }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void connectZookeeper(final String server) {
        Preconditions.checkNotNull(server);
        try {
            ZookeeperClient zookeeper = new ZookeeperClient(server);
            zookeeper.initDirectories();
            final String zkTaskManagerDir = ZookeeperClient.ZOOKEEPER_NODES + "/" + machine.machineID.toString();
            zookeeper.store(zkTaskManagerDir, machine);
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
                    zookeeper.getChildrenForPathAndWatch(ZookeeperClient.ZOOKEEPER_NODES, this);
                    final List<String> machineList = zookeeper.getChildrenForPath(ZookeeperClient.ZOOKEEPER_NODES);
                    // Find out whether a node was created or deleted.
                    //if (peers.values().size() < machineList.size()) {
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
