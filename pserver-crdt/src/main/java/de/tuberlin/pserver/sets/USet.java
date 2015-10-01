package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.sets.exceptions.NotUniqueException;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * The Unique Set assumes each value inserted into the set is unique. Hence, there is no need for a tombstone set.
 */

public class USet<T> extends AbstractSet<T> {
    private final Set<T> set = new HashSet<>();

    public USet(String id, DataManager dataManager) {
        super(id, dataManager);
        run(dataManager);
    }

    @Override
    protected boolean update(int srcNodeId, Operation<T> op, DataManager dm) {
        SetOperation<T> sop = (SetOperation<T>)op;

        if(sop.getType() == SetOperation.ADD) {
            return addElement(sop.getValue());
        }
        else if(sop.getType() == SetOperation.REMOVE) {
            return removeElement(sop.getValue());
        }
        else {
            return false;
        }
    }

    @Override
    public boolean add(T value, DataManager dataManager) {
        if(addElement(value)) {
            broadcast(new SetOperation<>(SetOperation.ADD, value), dataManager);
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(T value, DataManager dataManager) {
        if(removeElement(value)) {
            broadcast(new SetOperation<>(SetOperation.REMOVE, value), dataManager);
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
