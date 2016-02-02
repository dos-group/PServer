package de.tuberlin.pserver.crdt.sets;


import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

public abstract class AbstractSet<T> extends AbstractCRDT<T> implements Set<T> {

    public AbstractSet(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
    }

    public synchronized boolean contains(T element) {
        return getSet().contains(element);
    }
}