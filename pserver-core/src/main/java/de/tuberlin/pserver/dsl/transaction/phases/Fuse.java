package de.tuberlin.pserver.dsl.transaction.phases;


public interface Fuse<T> extends TransactionPhase<T> {

    public T fuse(final T[] remoteObjects) throws Exception;
}
