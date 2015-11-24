package de.tuberlin.pserver.dsl.transaction.phases;


import java.util.List;

public interface Update<I> extends Apply<I, Void> {

    public void update(final List<I> remoteObjects, final I localObject) throws Exception;

    // ---------------------------------------------------

    // Love Java 8 :)
    default public Void apply(final List<I> remoteObjects, final I localObject) throws Exception {
        update(remoteObjects, localObject);
        return null;
    }
}
