package de.tuberlin.pserver.sets;


import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.runtime.DataManager;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractSet<T> extends AbstractCRDT<T> implements SetCRDT {
    protected Set<T> value = new HashSet<T>();

    public AbstractSet(String id, DataManager dataManager) {
        super(id, dataManager);
    }

    public Set<T> getValue() {
        return this.value;
    }
}
