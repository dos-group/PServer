package de.tuberlin.pserver.crdt.sets;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.crdt.operations.SimpleOperation;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.crdt.exceptions.NotUniqueException;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.HashSet;
import java.util.Set;

/**
 * The Unique Set assumes each value inserted into the set is unique. Hence, there is no need for a tombstone set.
 */

public class USet<T> extends AbstractSet<T> {
    private final Set<T> set = new HashSet<>();

    public USet(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
    }

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        SimpleOperation<T> sop = (SimpleOperation<T>)op;

        if(sop.getType() == Operation.OpType.ADD) {
            return addElement(sop.getValue());
        }
        else if(sop.getType() == Operation.OpType.REMOVE) {
            return removeElement(sop.getValue());
        }
        else {
            return false;
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
    public Set<T> getSet() {
        return set;
    }

    private boolean addElement(T value) {
        if(!set.add(value)) {
            throw new NotUniqueException("The value "+ value + " is already contained in the Unique Set and cannot be " +
                    "added again.");
        }
        return true;
    }

    private boolean removeElement(T value) {
        return set.remove(value);
    }
}
