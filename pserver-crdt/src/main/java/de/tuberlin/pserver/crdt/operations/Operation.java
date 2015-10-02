package de.tuberlin.pserver.crdt.operations;

import org.apache.zookeeper.server.util.Profiler;

import java.util.Date;

public class Operation<T> implements IOperation<T> {
    private final int type;
    private final T value;

    public Operation(int type, T value) {
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
