package de.tuberlin.pserver.crdt.operations;

import de.tuberlin.pserver.crdt.CRDT;

public class EndOperation extends Operation<Integer> {

    public EndOperation() {
        super(CRDT.END, 1);
    }
}
