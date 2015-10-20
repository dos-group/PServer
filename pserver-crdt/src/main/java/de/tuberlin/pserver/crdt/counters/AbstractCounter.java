package de.tuberlin.pserver.crdt.counters;

import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.runtime.RuntimeManager;

/**
 * <p>
 *     This class provides a skeletal implementation of the {@code Counter} interface to minimize the effort required to
 *     implement this interface in subclasses. The count is initialized at zero.
 * </p>
 * <p>
 *     Classes extending this class should use the {@code addCount()} and {@code removeCount()} methods to modify the count.
 * </p>
 */
public abstract class AbstractCounter extends AbstractCRDT implements Counter {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private long count = 0;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    /**
     * Sole constructor.
     *
     * @param id the ID for this CRDT
     * @param runtimeManager the {@code RuntimeManager} belonging to this {@code MLProgram}
     */
    public AbstractCounter(String id, int noOfReplicas, RuntimeManager runtimeManager) {
        super(id, noOfReplicas, runtimeManager);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * Gets the count associated with this {@code Counter} CRDT.
     *
     * @return the count
     */
    @Override
    public long getCount() {
        // Primitives are passed by value (so are objects, but that is a longer story...)
        return count;
    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    /**
     * Increments the overall count by {@code i}
     *
     * @param i the amount to be added to the count
     * @return true if the count was changed by this operation
     */
    protected boolean incrementCount(int i) {
        if(i > 0) {
            count += i;
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * Decrements the overall count by {@code i}
     *
     * @param i the amount to be subtracted from the count
     * @return true if the count was changed by this operation
     */
    protected boolean decrementCount(int i) {
        if(i > 0) {
            count -= i;
            return true;
        }
        else {
            return false;
        }
    }
}
