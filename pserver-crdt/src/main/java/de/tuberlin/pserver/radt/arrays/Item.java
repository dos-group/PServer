package de.tuberlin.pserver.radt.arrays;

import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.radt.CObject;

public class Item<T> extends CObject<T> {
    private final int index;

    // no-arg constructor for serialization
    public Item() {
        this.index = -1;
    }

    public Item(int index, int[] vectorClock, S4Vector s4Vector, T value) {
        super(vectorClock, s4Vector, value);
        this.index = index;
    }


    public int getIndex() {
        return index;
    }
}
