package de.tuberlin.pserver.dsl.transaction.phases;


public interface Fuse extends TransactionPhase {

    public <T> T fuse(final T[] remoteObjects) throws Exception;
}
