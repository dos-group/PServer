package de.tuberlin.pserver.radt.arrays;

import de.tuberlin.pserver.radt.RADTOperation;
import de.tuberlin.pserver.radt.S4Vector;

public class ArrayOperation<T> extends RADTOperation<T> {
    private final int index;

    public ArrayOperation(OpType type, T value, int index, int[] vectorClock, S4Vector s4) {
        super(type, value, vectorClock, s4);
        this.index = index;
    }

    public int getIndex() {
        return index;
    }
}
