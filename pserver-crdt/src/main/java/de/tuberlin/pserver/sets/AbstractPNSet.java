package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.runtime.DataManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
            counterMap.put(value, ++count);
        }
        return true;
    }

    public boolean remove(T value) {
        Long count = counterMap.get(value);

        if(count == null) {
            counterMap.put(value, -1L);
        }
        else {
            counterMap.put(value, --count);
        }
        return true;
    }

    public Set<T> getSet() {
        return counterMap.keySet().stream().filter(key -> counterMap.get(key) > 0).collect(Collectors.toSet());
    }
}