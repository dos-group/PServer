package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.runtime.DataManager;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractTwoPSet<T> extends AbstractSet<T> {
    protected Set<T> tombstone = new HashSet<>();

    public AbstractTwoPSet(String id, DataManager dataManager) {
        super(id, dataManager);
    }

    protected boolean add(T item) {
        if(!tombstone.contains(item)) {
            return value.add(item);
        }
        return false;
    }

    protected boolean remove(T item) {
        if(value.remove(item)){
            tombstone.add(item);
            return true;
        }
        return false;
    }
}
