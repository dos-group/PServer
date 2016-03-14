package de.tuberlin.pserver.crdt.registers;

import de.tuberlin.pserver.crdt.CRDT;

public interface Register<T> extends CRDT<T> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    boolean set(T element);

    T get();

}
