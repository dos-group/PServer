package de.tuberlin.pserver.sets;


import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.runtime.DataManager;

import java.util.HashSet;
import java.util.Set;

// TODO: should this implement Set<E> from collections?
public abstract class AbstractSet<T> extends AbstractCRDT<T> implements SetCRDT {
    protected final Set<T> value;

    public AbstractSet(String id, DataManager dataManager) {
        super(id, dataManager);
        value = new HashSet<T>();
    }

    public Set<T> getValue() {
        return this.value;
    }
}