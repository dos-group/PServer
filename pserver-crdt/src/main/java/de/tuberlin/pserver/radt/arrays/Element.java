package de.tuberlin.pserver.radt.arrays;

import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.radt.CObject;

public class Element<T> extends CObject<T> {
    private final int index;

    // no-arg constructor for serialization
    public Element() {
        this.index = -1;
    }

    public Element(int index, S4Vector s4Vector, T value) {
        super(s4Vector, value);
        this.index = index;
    }


    public int getIndex() {
        return index;
    }
}
