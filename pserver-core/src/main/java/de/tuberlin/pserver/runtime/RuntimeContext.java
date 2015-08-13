package de.tuberlin.pserver.runtime;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.runtime.dht.DHTManager;

public final class RuntimeContext {

    // --------------------------------------------------
    // Fields.
    // --------------------------------------------------

    public final MachineDescriptor machine;

    public final int numOfNodes;

    public final int numOfSlots;

    public final int nodeID;

    public final NetManager netManager;

    public final DHTManager dht;

    public final DataManager dataManager;

    public final ExecutionManager executionManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public RuntimeContext(final MachineDescriptor machine,
                          final int numOfNodes,
                          final int numOfSlots,
                          final int nodeID,
                          final NetManager netManager,
                          final DHTManager dht,
                          final DataManager dataManager,
                          final ExecutionManager executionManager) {

        this.machine            = Preconditions.checkNotNull(machine);
        this.numOfNodes         = numOfNodes;
        this.numOfSlots         = numOfSlots;
        this.nodeID             = nodeID;
        this.dht                = Preconditions.checkNotNull(dht);
        this.netManager         = Preconditions.checkNotNull(netManager);
        this.dataManager        = Preconditions.checkNotNull(dataManager);
        this.executionManager   = Preconditions.checkNotNull(executionManager);
    }
}
