package de.tuberlin.pserver.crdt.sets;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.exceptions.IllegalOperationException;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.crdt.operations.SimpleOperation;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.HashSet;
import java.util.Set;

/**
 * In a Two-Phase Set an element may be added and removed but never added again thereafter.
 */

public class TwoPSet<T> extends AbstractSet<T> {
    private final Set<T> set;
    private final Set<T> tombstone;

    public TwoPSet(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);

        this.set = new HashSet<>();
        this.tombstone = new HashSet<>();
    }

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        // TODO: I hate this cast....
        SimpleOperation<T> sop = (SimpleOperation<T>)op;

        if(sop.getType() == Operation.OpType.ADD) {
            return addElement(sop.getValue());
        }
        else if(sop.getType() == Operation.OpType.REMOVE) {
            return removeElement(sop.getValue());
        }
        else {
            // TODO: specifiy exception.
            throw new IllegalOperationException("Blub");
        }
    }

    @Override
    public boolean add(T element) {
        if(addElement(element)) {
            broadcast(new SimpleOperation<T>(Operation.OpType.ADD, element));
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(T element) {
        if(removeElement(element)) {
            broadcast(new SimpleOperation<T>(Operation.OpType.REMOVE, element));
            return true;
        }
        return false;
    }

    @Override
    public Set<T> getSet() {
        return new HashSet<>(this.set);
    }

    private boolean addElement(T element) {
        if(!tombstone.contains(element)) {
            return set.add(element);
        }
        return false;
    }

    private boolean removeElement(T element) {
        if(set.remove(element)){
            tombstone.add(element);
            return true;
        }
        return false;
    }
}
