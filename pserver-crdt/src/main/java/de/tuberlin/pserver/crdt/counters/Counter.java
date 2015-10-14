package de.tuberlin.pserver.crdt.counters;

/**
 * <p>
 *     A counter which keeps a count and can be modified by the {@code increment} and {@code decrement} methods to
 *     increase or decrease its value.
 * </p>
 */
public interface Counter {

    /**
     * Gets the count associated with this counter.
     *
     * @return the count
     */
    long getCount();

    /**
     * Increments the the overall count by {@code i} and broadcasts the operation to other replicas if necessary.
     *
     * @param i the amount to be added to the count
     * @return true if {@code i} was successfully added to the count
     */
    boolean increment(int i);

    /**
     * Decrements the overall count by {@code i} and broadcasts the operation to other replicas if necessary.
     *
     * @param i the amount to be subtracted from the count
     * @return true if {@code i} was successfully subtracted from the count
     */
    boolean decrement(int i);
}
