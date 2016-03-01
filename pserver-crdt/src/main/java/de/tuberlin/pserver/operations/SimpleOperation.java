package de.tuberlin.pserver.operations;

import com.google.common.base.Preconditions;

import java.io.Serializable;

public class SimpleOperation<T> implements Operation<T>, Serializable {
    private final OpType type;
    private final T value;

    // No args constructor for Serialization (?)
    public SimpleOperation() {
        this.type = null;
        this.value = null;
    }

    public SimpleOperation(OpType type, T value) {
        Preconditions.checkNotNull(type, "Operation type can not be null");
        this.type = type;
        this.value = value;
    }

    @Override
    public OpType getType() {
        return this.type;
    }

    @Override
    public T getValue() {
        return this.value;
    }
}
