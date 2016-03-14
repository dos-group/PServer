package de.tuberlin.pserver.radt.arrays;

import de.tuberlin.pserver.radt.RADT;

public interface IArray<T> extends RADT {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    T read(int index);

    boolean write(int index, T value);

    Object[] getArray();

}