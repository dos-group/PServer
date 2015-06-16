package de.tuberlin.pserver.math.delegates;

import de.tuberlin.pserver.math.AbstractVector;

public abstract class DelegationVector<T> extends AbstractVector {

    protected final T target;

    public T getTarget() {
        return target;
    }

    public DelegationVector(long size, VectorType type, T target) {
        super(size, type);
        this.target = target;
    }


}
