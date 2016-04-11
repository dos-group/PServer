package de.tuberlin.pserver.dsl.transaction.phases;


import java.util.List;

public interface Combine<T> extends TransactionPhase {

    public T combine(final List<Object> requestObj, final List<T> remoteObjects) throws Exception;
}
