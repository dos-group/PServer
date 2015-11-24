package de.tuberlin.pserver.dsl.transaction.phases;


import java.util.List;

public interface GenericApply<I1, I2, O> extends TransactionPhase {

    public O apply(final List<I1> remoteObjects, final I2 localObject) throws Exception;
}