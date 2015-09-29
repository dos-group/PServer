package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.runtime.DataManager;

import java.io.Serializable;
import java.util.*;

// TODO: in add and remove, would it be ok to return false (i.e. not broadcast) if the update is not applied?
// TODO: think about concurrent add/remove/getset calls...
public abstract class AbstractLWWSet<T> extends AbstractSet<T> {
    private final Map<T, Long> addMap = new HashMap<>();
    private final Map<T, Long> removeMap = new HashMap<>();

    public AbstractLWWSet(String id, DataManager dataManager) {
        super(id, dataManager);
    }

    public boolean add(SetOperation<T> sop) {
        if (addMap.get(sop.getValue()) == null) {
            addMap.put(sop.getValue(), sop.getTime());
        }
        else if (sop.getTime() > addMap.get(sop.getValue())) {
            addMap.put(sop.getValue(), sop.getTime());
        }
        return true;
    }

    public boolean remove(SetOperation<T> sop) {
        if (removeMap.get(sop.getValue()) == null) {
            removeMap.put(sop.getValue(), sop.getTime());
        }
        else if (sop.getTime() > removeMap.get(sop.getValue())) {
            removeMap.put(sop.getValue(), sop.getTime());
        }
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
