package de.tuberlin.pserver.crdt.operations;

public class SimpleOperation<T> implements Operation<T> {
    private final int type;
    private final T value;

    public SimpleOperation(int type, T value) {
        this.type = type;
        this.value = value;
    }

    @Override
    public int getType() {
        return this.type;
    }

    @Override
    public T getValue() {
        return this.value;
    }
}
