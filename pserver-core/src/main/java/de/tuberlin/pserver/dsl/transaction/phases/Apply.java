package de.tuberlin.pserver.dsl.transaction.phases;


public interface Apply<T> extends TransactionPhase<T> {

    public T apply(final T object) throws Exception;
}
