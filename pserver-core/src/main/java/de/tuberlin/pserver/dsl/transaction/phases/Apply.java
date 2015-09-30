package de.tuberlin.pserver.dsl.transaction.phases;


import java.util.List;

public interface Apply<I, O> extends TransactionPhase {

    public O apply(final List<I> object) throws Exception;
}
