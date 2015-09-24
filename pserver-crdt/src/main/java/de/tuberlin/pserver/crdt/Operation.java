package de.tuberlin.pserver.crdt;

import java.io.Serializable;

public class Operation implements Serializable{
    private int type;

    public Operation(int type) {
        this.type = type;
    }

    public int getType() {
        return this.type;
    }
}