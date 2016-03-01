package de.tuberlin.pserver.operations;

import java.io.Serializable;

public class TaggedOperation<T,U> extends SimpleOperation<T> implements Serializable {
    private final U tag;

    // No args constructor for Serialization (?)
    public TaggedOperation() {
        this.tag = null;
    }

    public TaggedOperation(OpType type, T value, U tag) {
        super(type, value);
        this.tag = tag;
    }

    public U getTag() {
        return this.tag;
    }
}
