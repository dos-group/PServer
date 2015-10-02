package de.tuberlin.pserver.crdt.sets;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.exceptions.IllegalOperationException;
import de.tuberlin.pserver.crdt.operations.IOperation;
import de.tuberlin.pserver.crdt.operations.Operation;
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
    protected boolean update(int srcNodeId, IOperation<T> op) {
        Operation<T> sop = (Operation<T>)op;

        if(sop.getType() == CRDT.ADD) {
            return addElement(sop.getValue());
        }
        else {
            // TODO: formulate exception
            throw new IllegalOperationException("blub");
        }
    }

    @Override
    public boolean add(T element) {
        if(addElement(element)) {
            broadcast(new Operation<>(CRDT.ADD, element), dataManager);
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(T element) {
        if(removeElement(element)) {
            broadcast(new Operation<>(CRDT.REMOVE, element), dataManager);
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
