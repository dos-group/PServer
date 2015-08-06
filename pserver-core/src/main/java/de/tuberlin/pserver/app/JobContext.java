package de.tuberlin.pserver.app;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.dht.DHT;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.commons.ds.ResettableCountDownLatch;

import java.util.*;
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

    public final DHT dht;

    public final DataManager dataManager;

    public final ExecutionManager executionManager;


    public final ResettableCountDownLatch globalSyncBarrier;

    public final CyclicBarrier localSyncBarrier;

    public final List<InstanceContext> instanceContextList;

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
                      final DHT dht,
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
        this.instanceContextList = new ArrayList<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void addInstance(final InstanceContext ic) {
        instanceContextList.add(Preconditions.checkNotNull(ic));
    }

    public void removeInstance(final InstanceContext ic) {
        instanceContextList.remove(Preconditions.checkNotNull(ic));
    }
}
