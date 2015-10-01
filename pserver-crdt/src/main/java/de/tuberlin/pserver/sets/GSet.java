package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.crdt.IllegalOperationException;
import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.runtime.DataManager;

import java.util.HashSet;
import java.util.Set;

public class GSet<T> extends AbstractSet<T> {
    private final Set<T> set;

    public GSet(String id, DataManager dataManager) {
        super(id, dataManager);
        this.set = new HashSet<>();
        run(dataManager);
    }

    @Override
    protected boolean update(int srcNodeId, Operation<T> op, DataManager dm) {
        SetOperation<T> sop = (SetOperation<T>)op;

        if(sop.getType() == SetOperation.ADD) {
            return addElement(sop.getValue());
        }
        else {
            // TODO: formulate exception
            throw new IllegalOperationException("blub");
        }
    }

    @Override
    public boolean add(T element, DataManager dataManager) {
        if(addElement(element)) {
            broadcast(new SetOperation<>(SetOperation.ADD, element), dataManager);
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(T element, DataManager dataManager) {
        if(removeElement(element)) {
            broadcast(new SetOperation<>(SetOperation.REMOVE, element), dataManager);
            return true;
        }
        return false;
    }

    @Override
    public Set<T> getSet() {
        return this.set;
    }

    private boolean addElement(T element) {
        return set.add(element);
    }

    private boolean removeElement(T element) {
        return set.remove(element);
    }
}
