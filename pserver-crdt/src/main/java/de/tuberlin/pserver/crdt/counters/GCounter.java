package de.tuberlin.pserver.crdt.counters;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.crdt.operations.SimpleOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.io.Serializable;

/**
 * An increment-only implementation of the {@code Counter} interface. Calling {@code decrement()} on a {@code GCounter}
 * will cause an {@code OpperationNotSupportedException}.
 *
 */
public class GCounter extends AbstractCounter implements Counter, Serializable {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    /**
     * Sole Constructor.
     *
     * @param id the ID of this CRDT
     * @param programContext the {@code RuntimeManager} belonging to this {@code MLProgram}
     */
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

    private long remoteIncrement(int i) {
        return super.increment(i);
    }

    /**
     * The decrement operation is not supported by the {@code GCounter} CRDT. Calling this method causes an
     * {@code UnsupportedOperationException}.
     *
     * @param i the amount to be subtracted from the count
     * @return does not return
     * @throws UnsupportedOperationException if this method is called
     */
    @Override
    public long decrement(int i) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("The decrement() operation is not supported by the grow-only counter CRDT.");
    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    /**
     * Applies an {@code Operation} received from another replica to the local replica of a CRDT. {@code GCounter}
     * CRDTs only allow the INCREMENT operation. Any other operation causes an {@code IllegalArgumentException}.
     *
     * @param srcNodeID the ID of the node that broadcast the operation
     * @param op the operation to be applied locally
     * @return true if the update changed the value of the count
     */
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
}
