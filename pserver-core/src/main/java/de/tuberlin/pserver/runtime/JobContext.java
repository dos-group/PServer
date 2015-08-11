package de.tuberlin.pserver.runtime;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.ds.ResettableCountDownLatch;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.runtime.dht.DHTManager;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;

public class JobContext {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final MachineDescriptor clientMachine;

    public final UUID jobUID;

    public final String className;

    public final String simpleClassName;

    public final int numOfNodes;

    public final int numOfInstances;

    public final int nodeID;

    public final NetManager netManager;

    public final DHTManager dht;

    public final DataManager dataManager;

    public final ExecutionManager executionManager;

    public final ResettableCountDownLatch globalSyncBarrier;

    public final CyclicBarrier localSyncBarrier;

    public final List<SlotContext> slotContextList;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public JobContext(final MachineDescriptor clientMachine,
                      final UUID jobUID,
                      final String className,
                      final String simpleClassName,
                      final int numOfNodes,
                      final int numOfInstances,
                      final int nodeID,
                      final NetManager netManager,
                      final DHTManager dht,
                      final DataManager dataManager,
                      final ExecutionManager executionManager) {

        this.clientMachine      = Preconditions.checkNotNull(clientMachine);
        this.jobUID             = Preconditions.checkNotNull(jobUID);
        this.className          = Preconditions.checkNotNull(className);
        this.simpleClassName    = Preconditions.checkNotNull(simpleClassName);
        this.numOfNodes         = numOfNodes;
        this.numOfInstances     = numOfInstances;
        this.nodeID             = nodeID;
        this.dht                = Preconditions.checkNotNull(dht);
        this.netManager         = Preconditions.checkNotNull(netManager);
        this.dataManager        = Preconditions.checkNotNull(dataManager);
        this.executionManager   = Preconditions.checkNotNull(executionManager);
        this.globalSyncBarrier  = new ResettableCountDownLatch(numOfNodes);
        this.localSyncBarrier   = new CyclicBarrier(numOfInstances);
        this.slotContextList = new ArrayList<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void addInstance(final SlotContext ic) {
        slotContextList.add(Preconditions.checkNotNull(ic));
    }

    public void removeInstance(final SlotContext ic) {
        slotContextList.remove(Preconditions.checkNotNull(ic));
    }
}
