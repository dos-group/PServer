package de.tuberlin.pserver.radt;

import de.tuberlin.pserver.radt.S4Vector;

import java.io.Serializable;

public abstract class CObject<T> implements Serializable {
    private final int[] vectorClock;
    private S4Vector s4Vector;
    private T value;

    public CObject(int[] vectorClock, S4Vector s4Vector, T value) {
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

    public void setS4Vector(S4Vector vector) {
        this.s4Vector = vector;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }


}

