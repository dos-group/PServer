package de.tuberlin.pserver.crdt.sets;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.exceptions.IllegalOperationException;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.crdt.operations.SimpleOperation;
import de.tuberlin.pserver.runtime.RuntimeManager;

import java.util.HashSet;
import java.util.Set;

public class GSet<T> extends AbstractSet<T> {
    private final Set<T> set;

    public GSet(String id, RuntimeManager runtimeManager) {
        super(id, runtimeManager);
        this.set = new HashSet<>();
    }

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        SimpleOperation<T> sop = (SimpleOperation<T>)op;

        if(sop.getType() == Operation.ADD) {
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
            broadcast(new SimpleOperation<>(Operation.ADD, element));
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(T element) {
        if(removeElement(element)) {
            broadcast(new SimpleOperation<>(Operation.REMOVE, element));
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
