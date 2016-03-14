package de.tuberlin.pserver.crdt.sets;


import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

public abstract class AbstractSet<T> extends AbstractCRDT<T> implements Set<T> {

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractSet(String id, int noOfReplicas, ProgramContext programContext) {

        super(id, noOfReplicas, programContext);

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public synchronized boolean contains(T element) {

        return getSet().contains(element);

    }

}