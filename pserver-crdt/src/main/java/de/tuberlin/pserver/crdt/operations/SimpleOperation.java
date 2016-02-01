package de.tuberlin.pserver.crdt.operations;

import com.google.common.base.Preconditions;

public class SimpleOperation<T> implements Operation<T> {
    private final OpType type;
    private final T value;

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
