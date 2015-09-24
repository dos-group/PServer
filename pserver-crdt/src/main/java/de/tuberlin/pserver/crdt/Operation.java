package de.tuberlin.pserver.crdt;

import java.io.Serializable;

public class Operation implements Serializable{
    private static final int END = -1;
    private int type;

    public Operation(int type) {
        this.type = type;
    }

    public int getType() {
        return this.type;
    }

}
