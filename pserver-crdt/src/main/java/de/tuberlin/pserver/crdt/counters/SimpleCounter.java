package de.tuberlin.pserver.crdt.counters;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.operations.Operation;
import de.tuberlin.pserver.operations.SimpleOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

public class SimpleCounter extends AbstractCounter implements CRDT {

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    private SimpleCounter(String id, int noOfReplicas, ProgramContext programContext) {

        super(id, noOfReplicas, programContext);

        ready();

    }

    public static SimpleCounter newReplica(String id, int noOfReplicas, ProgramContext programContext) {

        return new SimpleCounter(id, noOfReplicas, programContext);

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
    public long decrement(int i) {

        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

        long count = super.decrement(i);

        broadcast(new SimpleOperation<>(Operation.OpType.DECREMENT, i));

        return count;

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

                super.increment(simpleOp.getValue());

                return true;

            case DECREMENT:

                super.decrement(simpleOp.getValue());

                return true;

            default:

                throw new IllegalArgumentException("SimpleCounter CRDTs do not allow the " + op.getType() + " operation.");
        }

    }

}
