package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.runtime.DataManager;

import java.io.Serializable;
import java.util.*;

public abstract class AbstractLWWSet<T> extends AbstractSet<T> {
    private Map<T, Date> addMap = new HashMap<>();
    private Map<T, Date> removeMap = new HashMap<>();

    public AbstractLWWSet(String id, DataManager dataManager) {
        super(id, dataManager);
    }

    public boolean add(Set<Pair<T>> items) {
        for(Pair<T> item : items) {
            addMap.put(item.getPairValue(), item.getPairTimestamp());
        }
        return true;
    }

    public boolean remove(Set<Pair<T>> items) {
        for(Pair<T> item : items) {
            removeMap.put(item.getPairValue(), item.getPairTimestamp());
        }
        return true;
    }

    public Set<T> getSet() {
        Set<T> set = new HashSet<T>();

        for(T key : addMap.keySet()) {
            if(removeMap.get(key) != null){
                if(addMap.get(key).after(removeMap.get(key))) {
                    set.add(key);
                }
            }
            else {
                set.add(key);
            }
        }
        return set;
    }

    public static class Pair<T> implements Serializable {
        private T value;
        private Date timestamp;

        public Pair(T value, Date timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }

        T getPairValue() {
            return value;
        }

        public Date getPairTimestamp() {
            return timestamp;
        }
    }
}
