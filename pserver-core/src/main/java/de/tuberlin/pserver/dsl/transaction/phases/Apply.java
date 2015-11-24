package de.tuberlin.pserver.dsl.transaction.phases;


import java.util.List;

public interface Apply<I, O> extends GenericApply<I, I, O> {

    public O apply(final List<I> remoteObjects, final I localObject) throws Exception;
}
