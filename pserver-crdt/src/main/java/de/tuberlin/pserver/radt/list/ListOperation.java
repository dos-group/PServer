package de.tuberlin.pserver.radt.list;

import de.tuberlin.pserver.radt.RADTOperation;
import de.tuberlin.pserver.radt.S4Vector;

public class ListOperation<T> extends RADTOperation<T> {
    // S4 vector of the node to the left of the inserted node. Used in insert operations to maintain intention
    private final S4Vector refS4;

    // no-args constructor for serialization
    public ListOperation() {
        this.refS4 = null;
    }

    public ListOperation(OpType type, T value, S4Vector referenceS4, int[] vectorClock, S4Vector s4) {
        super(type, value, vectorClock, s4);
        this.refS4 = referenceS4;
    }

    public S4Vector getRefS4Vector() {
        return refS4;
    }
}
