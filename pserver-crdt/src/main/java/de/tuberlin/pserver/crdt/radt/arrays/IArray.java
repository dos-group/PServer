package de.tuberlin.pserver.crdt.radt.arrays;

import de.tuberlin.pserver.crdt.radt.RADT;

public interface IArray<T> extends RADT<T> {

    T read(int index);
    boolean write(int index, T value);
}
