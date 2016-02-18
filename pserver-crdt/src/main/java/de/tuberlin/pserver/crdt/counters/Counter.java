package de.tuberlin.pserver.crdt.counters;

import de.tuberlin.pserver.crdt.CRDT;

// TODO: what about when counters reach MAX_INT => exception or keep counting somehow?

/**
 * <p>
 *     A counter which keeps a count and can be modified by the {@code increment} and {@code decrement} methods to
 *     increase or decrease its value.
 * </p>
 */
public interface Counter extends CRDT {

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
    long increment(int i);

    /**
     * Decrements the overall count by {@code i} and broadcasts the operation to other replicas if necessary.
     *
     * @param i the amount to be subtracted from the count
     * @return true if {@code i} was successfully subtracted from the count
     */
    long decrement(int i);
}
