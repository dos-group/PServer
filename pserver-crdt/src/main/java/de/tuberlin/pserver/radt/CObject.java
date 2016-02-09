package de.tuberlin.pserver.radt;

import java.io.Serializable;

public abstract class CObject<T> implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int[] vectorClock;
    private S4Vector s4Vector;
    private T value;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    // no-args constructor for serialization
    public CObject() {
        this.vectorClock = null;
        this.s4Vector = null;
        this.value = null;
    }

    public CObject(int[] vectorClock, S4Vector s4Vector, T value) {
        this.vectorClock = vectorClock;
        this.s4Vector = s4Vector;
        this.value = value;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

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

