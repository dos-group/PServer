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

    public final int numOfCores;

    public final int nodeID;

    public final NetManager netManager;

    public final DHTManager dhtManager;

    public final RuntimeManager runtimeManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public RuntimeContext(final MachineDescriptor machine,
                          final int numOfNodes,
                          final int numOfCores,
                          final int nodeID,
                          final NetManager netManager,
                          final DHTManager dhtManager,
                          final RuntimeManager runtimeManager) {

        this.machine            = Preconditions.checkNotNull(machine);
        this.numOfNodes         = numOfNodes;
        this.numOfCores         = numOfCores;
        this.nodeID             = nodeID;
        this.dhtManager         = Preconditions.checkNotNull(dhtManager);
        this.netManager         = Preconditions.checkNotNull(netManager);
        this.runtimeManager     = Preconditions.checkNotNull(runtimeManager);
    }
}
