package de.tuberlin.pserver.crdt;

import java.io.Serializable;

public class Operation implements Serializable {
    private int type;
    private int value;


    public Operation(int type, int value) {
        this.type = type;
        this.value = value;
    }

    public int getType() {
        return type;
    }

    public int getValue() {
        return value;
    }
}
