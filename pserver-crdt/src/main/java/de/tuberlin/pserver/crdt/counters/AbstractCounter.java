package de.tuberlin.pserver.crdt.counters;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

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

    private long count;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    /**
     * Sole constructor.
     *
     * @param id the ID for this CRDT
     * @param programContext the {@code RuntimeManager} belonging to this {@code MLProgram}
     */
    public AbstractCounter(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
        this.count = 0;
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
    public synchronized long getCount() {
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
    @Override
    public synchronized long increment(int i) {
        Preconditions.checkArgument(i > 0, "The method increment() can not be invoked with 0 or a negative value: " + i);

        count = Math.addExact(count, i);
        return count;
    }

    /**
     * Decrements the overall count by {@code i}
     *
     * @param i the amount to be subtracted from the count
     * @return true if the count was changed by this operation
     */
    @Override
    public synchronized long decrement(int i) {
        Preconditions.checkArgument(i > 0, "The method decrement() can not be invoked with 0 or a negative value: " + i);

        count = Math.subtractExact(count, i);
        return count;
    }
}
