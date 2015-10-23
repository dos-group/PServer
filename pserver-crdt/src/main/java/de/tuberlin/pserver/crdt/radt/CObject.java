package de.tuberlin.pserver.crdt.radt;

public class CObject<T> {
    private final int index;
    private final int[] vectorClock;
    private final S4Vector s4Vector;
    private final T value;

    public CObject(int index, int[] vectorClock, S4Vector s4Vector, T value) {
        this.index = index;
        this.vectorClock = vectorClock;
        this.s4Vector = s4Vector;
        this.value = value;
    }

    public int[] getVectorClock() {
        return vectorClock;
    }

    public S4Vector getS4Vector() {
        return s4Vector;
    }

    public T getValue() {
        return value;
    }

    public int getIndex() {
        return index;
    }
}

