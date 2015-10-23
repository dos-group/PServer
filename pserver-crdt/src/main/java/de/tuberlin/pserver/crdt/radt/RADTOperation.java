package de.tuberlin.pserver.crdt.radt;


import de.tuberlin.pserver.crdt.operations.SimpleOperation;

public class RADTOperation<V> extends SimpleOperation<V> {
    private final int index;
    private final int[] vectorClock;
    private final S4Vector s4;


    public RADTOperation(int type, V value, int index, int[] vectorClock, S4Vector s4) {
        super(type, value);
        this.index = index;
        this.vectorClock = vectorClock;
        this.s4 = s4;
    }

    public int[] getVectorClock() {
        return vectorClock;
    }

    public S4Vector getS4Vector() {
        return s4;
    }

    public int getIndex() {
        return index;
    }
}
