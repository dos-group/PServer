package de.tuberlin.pserver.operations;

public class EndOperation extends SimpleOperation<Integer> {

    public EndOperation() {
        super(Operation.OpType.END, 1);
    }
}
