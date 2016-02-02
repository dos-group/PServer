package de.tuberlin.pserver.crdt.registers;

import de.tuberlin.pserver.crdt.CRDT;

public interface Register<T> extends CRDT<T> {

    boolean set(T element);
    T get();

}
