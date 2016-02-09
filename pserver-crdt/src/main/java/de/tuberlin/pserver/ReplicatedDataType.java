package de.tuberlin.pserver;

import de.tuberlin.pserver.crdt.operations.Operation;

public interface ReplicatedDataType<T> {

    /**
     * Signals that this replica has finished broadcasting updates. It then waits to receive the
     * {@link Operation.OpType#END END}
     * token from all other existing replicas and applies all outstanding operations to reach the final state of this CRDT.
     */
    void finish();
    //void ready();
    //ReplicatedDataType newReplica();
}
