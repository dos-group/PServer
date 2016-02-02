package de.tuberlin.pserver.crdt.sets;

import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.crdt.operations.SimpleOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.HashSet;
import java.util.Set;

// Grow only Set
public class GSet<T> extends AbstractSet<T> {
    private final Set<T> set;

    public GSet(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
        this.set = new HashSet<>();
    }

    @Override
    public boolean add(T element) {
        if(addElement(element)) {
            broadcast(new SimpleOperation<>(Operation.OpType.ADD, element));
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(T element) {
        throw new UnsupportedOperationException("The remove() operation is not supported by the grow-only set CRDT.");
    }

    @Override
    public synchronized Set<T> getSet() {
        // Defensive copy
        return new HashSet<>(this.set);
    }

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        @SuppressWarnings("unchecked")
        SimpleOperation<T> simpleOp = (SimpleOperation<T>)op;

        switch(simpleOp.getType()) {
            case ADD:
                return addElement(simpleOp.getValue());
            default:
                throw new IllegalArgumentException("GSet CRDTs do not allow the " + op.getType() + " operation.");
        }
    }

    private synchronized boolean addElement(T element) {
        return set.add(element);
    }
}
