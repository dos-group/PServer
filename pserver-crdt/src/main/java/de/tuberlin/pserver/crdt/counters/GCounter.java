package de.tuberlin.pserver.crdt.counters;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.exceptions.IllegalOperationException;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.crdt.operations.SimpleOperation;
import de.tuberlin.pserver.runtime.RuntimeManager;

import javax.naming.OperationNotSupportedException;
import java.io.Serializable;

/**
 * An increment-only implementation of the {@code Counter} interface. Calling {@code decrement()} on a {@code GCounter}
 * will cause an {@code OpperationNotSupportedException}.
 *
 */
public class GCounter extends AbstractCounter implements CRDT, Serializable {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    /**
     * Sole Constructor.
     *
     * @param id the ID of this CRDT
     * @param runtimeManager the {@code RuntimeManager} belonging to this {@code MLProgram}
     */
    public GCounter(String id, int noOfReplicas, RuntimeManager runtimeManager) {
        super(id, noOfReplicas, runtimeManager);
    }

    // ---------------------------------------------------
    // Public Methods.
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
        SimpleOperation<Integer> cop = (SimpleOperation<Integer>) op;

        if (cop.getType() == Operation.INCREMENT) {
            return incrementCount(cop.getValue());
        } else {
            throw new IllegalArgumentException("GCounter CRDTs do not allow the " + op.getOperationType() + " operation.");
        }
    }

    @Override
    public boolean increment(int i) {
        if(incrementCount(i)) {
            broadcast(new SimpleOperation<>(Operation.INCREMENT, i));
            return true;
        }
        return false;
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
    public boolean decrement(int i) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }
}
