package de.tuberlin.pserver.crdt.operations;

public class TaggedOperation<T,U> extends SimpleOperation<T> {
    private final U tag;

    public TaggedOperation(OpType type, T value, U tag) {
        super(type, value);
        this.tag = tag;
    }

    public U getTag() {
        return this.tag;
    }
}
