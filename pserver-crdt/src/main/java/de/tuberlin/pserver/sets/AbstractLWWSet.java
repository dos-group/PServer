package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.runtime.DataManager;

import java.io.Serializable;
import java.util.*;

public abstract class AbstractLWWSet<T> extends AbstractSet<T> {
    private Map<T, Long> addMap = new HashMap<>();
    private Map<T, Long> removeMap = new HashMap<>();

    public AbstractLWWSet(String id, DataManager dataManager) {
        super(id, dataManager);
    }

    public boolean add(SetOperation<T> sop) {
        addMap.put(sop.getValue(), sop.getTime());
        return true;
    }

    public boolean remove(SetOperation<T> sop) {
        removeMap.put(sop.getValue(), sop.getTime());
        return true;
    }

    public Set<T> getSet() {
        Set<T> set = new HashSet<T>();

        for(T key : addMap.keySet()) {
            if(removeMap.get(key) != null){
                if(addMap.get(key) > removeMap.get(key)) {
                    set.add(key);
                }
            }
            else {
                set.add(key);
            }
        }
        return set;
    }
}
