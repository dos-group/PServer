package de.tuberlin.pserver.dsl.transaction;


public interface TransactionObserver<T> {

    boolean observe(final T[] srcObjects);
}
