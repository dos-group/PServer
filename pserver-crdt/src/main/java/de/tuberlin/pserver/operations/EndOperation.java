package de.tuberlin.pserver.operations;

public class EndOperation extends SimpleOperation<Integer> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public EndOperation() {

        super(Operation.OpType.END, 1);

    }

}
