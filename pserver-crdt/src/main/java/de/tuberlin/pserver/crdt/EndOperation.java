package de.tuberlin.pserver.crdt;

public class EndOperation extends AbstractOperation {

    public EndOperation() {
        super(CRDT.END, 1);
    }
}
