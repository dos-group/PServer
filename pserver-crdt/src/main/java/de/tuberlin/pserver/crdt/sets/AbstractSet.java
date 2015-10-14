package de.tuberlin.pserver.crdt.sets;


import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.runtime.DataManager;

// TODO: should this implement ISet<E> from collections library?
public abstract class AbstractSet<T> extends AbstractCRDT<T> implements ISet<T> {

    public AbstractSet(String id, DataManager dataManager) {
        super(id, dataManager);
    }

    public boolean contains(T element) {
        return getSet().contains(element);
    }
}