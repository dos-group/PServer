package de.tuberlin.pserver.dsl.transaction.phases;


public interface Prepare<I, O> extends TransactionPhase {

    public O prepare(final Object requestObj, final I object) throws Exception;
}
