package de.tuberlin.pserver.crdt.sets;

import de.tuberlin.pserver.crdt.exceptions.NotUniqueException;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.crdt.operations.SimpleOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.HashSet;
import java.util.Set;

/**
 * The Unique Set assumes each value inserted into the set is unique. Hence, there is no need for a tombstone set.
 * However, add must always be delivered before remove!
 */

public class USet<T> extends AbstractSet<T> {
    private final Set<T> set;

    public USet(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
        
        this.set = new HashSet<>();
        ready();
    }

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        @SuppressWarnings("unchecked")
        SimpleOperation<T> simpleOp = (SimpleOperation<T>)op;

        switch(simpleOp.getType()) {
            case ADD:
                return addElement(simpleOp.getValue());
            case REMOVE:
                return removeElement(simpleOp.getValue());
            default:
                throw new IllegalArgumentException("USet CRDTs do not allow the " + op.getType() + " operation.");

        }
    }

    @Override
    public boolean add(T value) {
        if(addElement(value)) {
            broadcast(new SimpleOperation<>(Operation.OpType.ADD, value));
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(T value) {
        if(removeElement(value)) {
            broadcast(new SimpleOperation<>(Operation.OpType.REMOVE, value));
            return true;
        }
        return false;
    }

    @Override
    public synchronized Set<T> getSet() {
        return new HashSet<>(set);
    }

    private synchronized boolean addElement(T value) {
        if(!set.add(value)) {
            throw new NotUniqueException("The value "+ value + " is already contained in the Unique Set and cannot be " +
                    "added again.");
        }
        return true;
    }

    private synchronized boolean removeElement(T value) {
        return set.remove(value);
    }
}
