package de.tuberlin.pserver.crdt.sets;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.exceptions.IllegalOperationException;
import de.tuberlin.pserver.crdt.operations.IOperation;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.runtime.DataManager;

import java.util.HashSet;
import java.util.Set;

/**
 * In a Two-Phase ISet an element may be added and removed but never added again thereafter.
 */

public class TwoPSet<T> extends AbstractSet<T> {
    private final Set<T> set;
    private final Set<T> tombstone;

    public TwoPSet(String id, DataManager dataManager) {
        super(id, dataManager);

        this.set = new HashSet<>();
        this.tombstone = new HashSet<>();

        run(dataManager);
    }

    @Override
    protected boolean update(int srcNodeId, IOperation<T> op) {
        // TODO: I hate this cast....
        Operation<T> sop = (Operation<T>)op;

        if(sop.getType() == CRDT.ADD) {
            return addElement(op.getValue());
        }
        else if(sop.getType() == CRDT.REMOVE) {
            return removeElement(op.getValue());
        }
        else {
            // TODO: specifiy exception.
            throw new IllegalOperationException("Blub");
        }
    }

    @Override
    public boolean add(T element) {
        if(addElement(element)) {
            broadcast(new Operation<T>(CRDT.ADD, element), dataManager);
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(T element) {
        if(removeElement(element)) {
            broadcast(new Operation<T>(CRDT.REMOVE, element), dataManager);
            return true;
        }
        return false;
    }

    @Override
    public Set<T> getSet() {
        return this.set;
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
