package de.tuberlin.pserver.dsl.transaction.phases;


public interface Prepare<T> extends TransactionPhase<T> {

    public T prepare(final T object) throws Exception;
}
