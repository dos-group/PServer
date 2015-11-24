package de.tuberlin.pserver.crdt.sets;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.crdt.operations.TaggedOperation;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.*;

/**
 * An element is in the set if it is in the add-Set and not in the remove-Set with a higher timestamp.
 */
// TODO: what about if this grows infinitely until it is too large for memory? Manual Garbage collection somehow?
public class LWWSet<T> extends AbstractSet<T> {
    private final Map<T, Long> addMap;
    private final Map<T, Long> removeMap;

    public LWWSet(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
        
        addMap = new HashMap<>();
        removeMap = new HashMap<>();
    }

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        TaggedOperation<T,Date> lwws = (TaggedOperation<T,Date>) op;

        if(lwws.getType() == Operation.ADD) {
            return addElement(lwws.getValue(), lwws.getTag().getTime());
        }
        else if(lwws.getType() == Operation.REMOVE) {
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
            broadcast(new TaggedOperation<>(Operation.ADD, element, time));
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(T element) {
        Date time = Calendar.getInstance().getTime();
        if(removeElement(element, time.getTime())) {
            broadcast(new TaggedOperation<>(Operation.REMOVE, element, time));
            return true;
        }
        return false;
    }

    @Override
    public java.util.Set<T> getSet() {
        java.util.Set<T> set = new HashSet<T>();

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
