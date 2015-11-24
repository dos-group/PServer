package de.tuberlin.pserver.crdt.counters;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.exceptions.IllegalOperationException;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.crdt.operations.SimpleOperation;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

/**
 * An implementation of the {@code Counter} interface which fully supports calls to {@code increment()} and
 * {@code decrement()}. There are no bounds this counter's value (beyond the bounds of the {@code long} data type).
 */
public class SimpleCounter extends AbstractCounter implements CRDT {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    /**
     * Sole constructor.
     *
     * @param id the ID for this CRDT
     * @param programContext the {@code ProgramContext} belonging to this {@code MLProgram}
     */
    public SimpleCounter(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * Applies an {@code Operation} received from another replica to the local replica of a CRDT. {@code SimpleCounter}
     * CRDTs only allow the INCREMENT or DECREMENT operations. Any other operation causes an {@code IllegalArgumentException}.
     *
     * @param srcNodeID the ID of the node that broadcast the operation
     * @param op the operation to be applied locally
     * @return true if the update changed the value of the count
     */
    @Override
    protected boolean update(int srcNodeID, Operation op) {
        @SuppressWarnings("unchecked")
        SimpleOperation<Integer> cop = (SimpleOperation<Integer>) op;

        if(cop.getType() == Operation.INCREMENT) {
            return incrementCount(cop.getValue());
        }
        else if(cop.getType() == Operation.DECREMENT) {
            return decrementCount(cop.getValue());
        }
        else {
            // TODO: throw a specific exception message
            throw new IllegalOperationException("Blub");
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

    @Override
    public boolean decrement(int i) {
        if(decrementCount(i)) {
            broadcast(new SimpleOperation<>(Operation.DECREMENT, i));
            return true;
        }
        return false;
    }
}
