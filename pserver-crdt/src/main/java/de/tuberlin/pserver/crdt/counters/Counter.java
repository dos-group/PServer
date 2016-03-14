package de.tuberlin.pserver.crdt.counters;

import de.tuberlin.pserver.crdt.CRDT;

public interface Counter extends CRDT {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    long getCount();

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    long increment(int i);

    long decrement(int i);
}
