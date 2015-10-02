package de.tuberlin.pserver.crdt.sets;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.operations.IOperation;
import de.tuberlin.pserver.crdt.operations.TaggedOperation;
import de.tuberlin.pserver.runtime.DataManager;

import java.util.*;

/**
 * An element is in the set if it is in the add-ISet and not in the remove-ISet with a higher timestamp.
 */
// TODO: what about if this grows infinitely until it is too large for memory? Manual Garbage collection somehow?
public class LWWSet<T> extends AbstractSet<T> {
    private final Map<T, Long> addMap;
    private final Map<T, Long> removeMap;

    public LWWSet(String id, DataManager dataManager) {
        super(id, dataManager);
        
        addMap = new HashMap<>();
        removeMap = new HashMap<>();
        
        run(dataManager);
    }

    @Override
    protected boolean update(int srcNodeId, IOperation<T> op) {
        TaggedOperation<T,Date> lwws = (TaggedOperation<T,Date>) op;

        if(lwws.getType() == CRDT.ADD) {
            return addElement(lwws.getValue(), lwws.getTag().getTime());
        }
        else if(lwws.getType() == CRDT.REMOVE) {
            return removeElement(lwws.getValue(), lwws.getTag().getTime());
        }
        else {
            return false;
        }
    }

    @Override
    public boolean add(T element) {
        Date time = Calendar.getInstance().getTime();
        if(addElement(element, time.getTime())) {
            broadcast(new TaggedOperation<>(CRDT.ADD, element, time), dataManager);
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(T element) {
        Date time = Calendar.getInstance().getTime();
        if(removeElement(element, time.getTime())) {
            broadcast(new TaggedOperation<>(CRDT.REMOVE, element, time), dataManager);
            return true;
        }
        return false;
    }

    @Override
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

    private boolean addElement(T element, long time) {
        if (addMap.get(element) == null) {
            addMap.put(element, time);
        }
        else if (time > addMap.get(element)) {
            addMap.put(element, time);
        }
        return true;
    }

    private boolean removeElement(T element, long time) {
        if (removeMap.get(element) == null) {
            removeMap.put(element, time);
        }
        else if (time > removeMap.get(element)) {
            removeMap.put(element, time);
        }
        return true;
    }
}
