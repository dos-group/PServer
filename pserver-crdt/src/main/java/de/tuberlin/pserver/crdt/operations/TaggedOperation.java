package de.tuberlin.pserver.crdt.operations;

public class TaggedOperation<T,U> extends Operation<T> {
    private final U tag;

    public TaggedOperation(int type, T value, U tag) {
        super(type, value);
        this.tag = tag;
    }

    public U getTag() {
        return this.tag;
    }
}
