package de.tuberlin.pserver.crdt.operations;


import de.tuberlin.pserver.crdt.operations.AbstractOperation;

public class CounterOperation extends AbstractOperation<Integer> {
    public CounterOperation(int type, int value) {
        super(type, value);
    }
}
