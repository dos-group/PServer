package de.tuberlin.pserver.counters;


import de.tuberlin.pserver.crdt.AbstractOperation;

public class CounterOperation extends AbstractOperation<Integer> {
    public static final int ADD = 0;
    public static final int SUBTRACT = 1;

    public CounterOperation(int type, int value) {
        super(type, value);
    }
}
