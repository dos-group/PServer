package de.tuberlin.pserver.crdt.operations;

import de.tuberlin.pserver.crdt.CRDT;

public class EndOperation extends SimpleOperation<Integer> {

    public EndOperation() {
        super(OpType.END, 1);
    }
}
