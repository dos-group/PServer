package de.tuberlin.pserver.crdt;

public class AbstractOperation<T> implements Operation<T> {
    private final int type;
    private final T value;

    public AbstractOperation(int type, T value) {
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
