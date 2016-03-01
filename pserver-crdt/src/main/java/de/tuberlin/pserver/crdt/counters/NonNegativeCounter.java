package de.tuberlin.pserver.crdt.counters;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.operations.Operation;
import de.tuberlin.pserver.operations.SimpleOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

/**
 * An implementation of the {@code Counter} interface which supports calls to {@code increment()} and
 * {@code decrement()} but never allows the count to become negative.
 */
/*
 * I chose the implementation method where even if the internal counter is negative, the CRDT returns value 0. However,
 * beware that if the internal counter is -1 and increment() is called the value of the CRDT will still be 0!
 */
public class NonNegativeCounter extends AbstractCounter implements Counter{

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    /**
     * Sole constructor.
     *
     * @param id the ID for this CRDT
     * @param programContext the {@code ProgramContext} belonging to this {@code MLProgram}
     */
    public NonNegativeCounter(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
        ready();
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

   @Override
   public synchronized long getCount() {
       long count = super.getCount();
       return count > 0 ? count : 0;
   }



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
