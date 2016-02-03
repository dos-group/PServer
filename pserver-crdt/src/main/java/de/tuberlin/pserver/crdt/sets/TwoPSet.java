package de.tuberlin.pserver.crdt.sets;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.exceptions.IllegalOperationException;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.crdt.operations.SimpleOperation;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * In a Two-Phase Set an element may be added and removed but never added again thereafter.
 */

public class TwoPSet<T> extends AbstractSet<T> {
    private final Set<T> set;
    private final Set<T> tombstoneSet;

    public TwoPSet(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);

        this.set = new HashSet<>();
        this.tombstoneSet = new HashSet<>();
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
                throw new IllegalArgumentException("2PSet CRDTs do not allow the " + op.getType() + " operation.");

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
    public synchronized Set<T> getSet() {
        return set.stream()
                .filter(val -> !tombstoneSet.contains(val))
                .collect(Collectors.toSet());
    }

    private synchronized boolean addElement(T element) {
        if(!tombstoneSet.contains(element)) {
            set.add(element);
            return true;
        }
        return false;
    }

    private synchronized boolean removeElement(T element) {
        if(set.contains(element) && !tombstoneSet.contains(element)){
            tombstoneSet.add(element);
            return true;
        }
        return false;
    }
}
