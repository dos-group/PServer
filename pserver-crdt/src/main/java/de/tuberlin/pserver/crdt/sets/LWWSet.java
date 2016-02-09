package de.tuberlin.pserver.crdt.sets;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.crdt.operations.TaggedOperation;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * An element is in the set if it is in the add-Set and not in the remove-Set with a higher timestamp.
 */
// TODO: what about if this grows infinitely until it is too large for memory? Manual Garbage collection somehow?
/**
 * TODO: At the moment remove takes precedent with concurrent operations. Perhaps allow a flag for the user to choose if
 *  add or remove should take precedent
 */
public class LWWSet<T> extends AbstractSet<T> {
    private final Map<T, Long> addMap;
    private final Map<T, Long> removeMap;

    public LWWSet(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);

        addMap = new ConcurrentHashMap<>();
        removeMap = new ConcurrentHashMap<>();
        ready();
    }

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        @SuppressWarnings("unchecked")
        TaggedOperation<T,Long> taggedOp = (TaggedOperation<T,Long>) op;

        switch(taggedOp.getType()) {
            case ADD:
                return addElement(taggedOp.getValue(), taggedOp.getTag());
            case REMOVE:
                return removeElement(taggedOp.getValue(), taggedOp.getTag());
            default:
                throw new IllegalArgumentException("LWWSet CRDTs do not allow the " + op.getType() + " operation.");

        }
    }

    @Override
    public boolean add(T element) {
        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

        long timestamp = System.nanoTime();
        if(addElement(element, timestamp)) {
            broadcast(new TaggedOperation<>(Operation.OpType.ADD, element, timestamp));
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(T element) {
        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

        long timestamp = System.nanoTime();
        if(removeElement(element, timestamp)) {
            broadcast(new TaggedOperation<>(Operation.OpType.REMOVE, element, timestamp));
            return true;
        }
        return false;
    }

    @Override
    public synchronized java.util.Set<T> getSet() {
        return addMap.keySet().stream()
                .filter(key -> !removeMap.containsKey(key) || addMap.get(key) > removeMap.get(key))
                .collect(Collectors.toSet());
    }

    private synchronized boolean addElement(T element, long time) {
        Long oldVal = addMap.putIfAbsent(element, time);
        if(oldVal == null) return true;

        if(time > oldVal) {
            addMap.replace(element, time);
            return true;
        }

        return false;
    }

    private synchronized boolean removeElement(T element, long time) {
        Long oldVal = removeMap.putIfAbsent(element, time);
        if(oldVal == null) return true;

        if(time > oldVal) {
            removeMap.replace(element, time);
            return true;
        }

        return false;
    }
}
