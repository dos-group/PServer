package de.tuberlin.pserver.crdt.counters;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.operations.Operation;
import de.tuberlin.pserver.operations.SimpleOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.io.Serializable;

public class GCounter extends AbstractCounter implements Counter, Serializable {

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    private GCounter(String id, int noOfReplicas, ProgramContext programContext) {

        super(id, noOfReplicas, programContext);

        ready();

    }

    public static GCounter newReplica(String id, int noOfReplicas, ProgramContext programContext) {

        return new GCounter(id, noOfReplicas, programContext);

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public long increment(int i) {

        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

        long count = super.increment(i);

        broadcast(new SimpleOperation<>(Operation.OpType.INCREMENT, i));

        return count;

    }


    @Override
    public long decrement(int i) throws UnsupportedOperationException {

        throw new UnsupportedOperationException("The decrement() operation is not supported by the grow-only counter CRDT.");

    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------


    @Override
    protected boolean update(int srcNodeID, Operation op) {

        @SuppressWarnings("unchecked")
        SimpleOperation<Integer> simpleOp = (SimpleOperation<Integer>) op;

        switch(simpleOp.getType()) {

            case INCREMENT:

                remoteIncrement(simpleOp.getValue());

                return true;

            default:

                throw new IllegalArgumentException("GCounter CRDTs do not allow the " + op.getType() + " operation.");

        }

    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private long remoteIncrement(int i) {

        return super.increment(i);

    }

}
