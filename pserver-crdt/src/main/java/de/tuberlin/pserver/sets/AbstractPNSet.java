package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.runtime.DataManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// TODO: What if the map just keeps growing over time? Perhaps there should be some garbage collection.
public abstract class AbstractPNSet<T> extends AbstractSet<T> {
    private final Map<T, Long> counterMap = new HashMap<>();

    public AbstractPNSet(String id, DataManager dataManager) {
        super(id, dataManager);
    }

    public boolean add(T value) {
        Long count = counterMap.get(value);

        if(count == null) {
            counterMap.put(value, 1L);
        }
        else {
            counterMap.put(value, count++);
        }
        return true;
    }

    public boolean remove(T value) {
        Long count = counterMap.get(value);

        if(count == null) {
            counterMap.put(value, -1L);
        }
        else {
            counterMap.put(value, count--);
        }
        return true;
    }

    public Set<T> getSet() {
        Set<T> set = new HashSet<>();

        for(T key : counterMap.keySet()) {
            if(counterMap.get(key) > 0) {
                set.add(key);
            }
        }

        return set;
    }
}