package de.tuberlin.pserver.radt.arrays;

import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.radt.CObject;

public class Element<T> extends CObject<T> {

    public Element(T value, S4Vector s4Vector) {

        super(s4Vector, value);

    }
}
