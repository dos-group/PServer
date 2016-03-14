package de.tuberlin.pserver;

import de.tuberlin.pserver.operations.Operation;

public interface ReplicatedDataType<T> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * Signals that this replica has finished broadcasting updates. It then waits to receive the
     * {@link Operation.OpType#END END}
     * token from all other existing replicas and applies all outstanding operations to reach the final state of this CRDT.
     */
    void finish();
}
