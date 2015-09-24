package de.tuberlin.pserver.counters;


import de.tuberlin.pserver.crdt.Operation;

import java.io.Serializable;

public class CounterOperation extends Operation {
    public static final int ADD = 0;
    public static final int SUBTRACT = 1;
    private int value;


    public CounterOperation(int type, int value) {
        super(type);
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
