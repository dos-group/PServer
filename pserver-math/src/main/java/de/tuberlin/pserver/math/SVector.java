package de.tuberlin.pserver.math;

import de.tuberlin.pserver.math.delegates.DelegationVector;

public abstract class SVector<T> extends DelegationVector<T> {

    public SVector(long size, VectorType type, T target) {
        super(size, type, target);
    }
}
