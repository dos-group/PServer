package de.tuberlin.pserver.crdt.counters;

/**
 * An implementation of the {@code Counter} interface which supports calls to {@code increment()} and
 * {@code decrement()} but never allows the count to become negative.
 */
public class NonNegativeCounter {
    /*
    This is difficult because it mean upholding a global invariant (count >= 0). Therefore there has to be a tradeoff.
    Possible solutions are:
        1. allow any amount of decrements but always return a count of 0 if a negative count would have occurred
        2. enforce local invariant => no node may issue more decrements than it issued increments
        3. synchronize by allowing nodes to reserve the right to a certain amount of decrements
     */
    // TODO: Do we need this?
}
