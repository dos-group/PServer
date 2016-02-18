package de.tuberlin.pserver.radt;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;

public abstract class CObject<T> implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private S4Vector s4Vector;
    private T value;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    // no-args constructor for serialization
    public CObject() {
        this.s4Vector = null;
        this.value = null;
    }

    public CObject(S4Vector s4Vector, T value) {
        this.s4Vector = s4Vector;
        this.value = value;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public synchronized S4Vector getS4Vector() {
        return s4Vector;
    }

    public synchronized void setS4Vector(S4Vector vector) {
        this.s4Vector = vector;
    }

    public synchronized T getValue() {
        return value;
    }

    public synchronized void setValue(T value) {
        this.value = value;
    }

    public synchronized boolean isTombstone() {
        return getValue() == null;
    }

    public synchronized void makeTombstone() {
        setValue(null);
    }

    @Override
    public synchronized boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof CObject)) return false;

        CObject<?> other = (CObject<?>) obj;

        return new EqualsBuilder()
                .append(this.s4Vector, other.s4Vector)
                .append(this.value, other.value)
                .isEquals();
    }

    @Override
    public synchronized int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(s4Vector)
                .append(value)
                .toHashCode();
    }
}

