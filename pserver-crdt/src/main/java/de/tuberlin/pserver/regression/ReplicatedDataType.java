package de.tuberlin.pserver.regression;

import de.tuberlin.pserver.crdt.operations.Operation;

public interface ReplicatedDataType {

    /**
     * Signals that this replica has finished broadcasting updates. It then waits to receive the
     * {@link Operation#END END}
     * token from all other existing replicas and applies all outstanding operations to reach the final state of this CRDT.
     */
    void finish();
    //ReplicatedDataType newReplica();
}
