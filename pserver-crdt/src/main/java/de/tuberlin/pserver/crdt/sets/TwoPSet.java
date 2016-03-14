package de.tuberlin.pserver.crdt.sets;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.operations.Operation;
import de.tuberlin.pserver.operations.SimpleOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * In a Two-Phase Set an element may be added and removed but never added again thereafter.
 */

public class TwoPSet<T> extends AbstractSet<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Set<T> set;

    private final Set<T> tombstoneSet;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TwoPSet(String id, int noOfReplicas, ProgramContext programContext) {

        super(id, noOfReplicas, programContext);

        this.set = new HashSet<>();

        this.tombstoneSet = new HashSet<>();

        ready();

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public boolean add(T element) {

        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

        if(addElement(element)) {

            broadcast(new SimpleOperation<T>(Operation.OpType.ADD, element));

            return true;

        }

        return false;

    }

    @Override
    public boolean remove(T element) {

        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

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

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

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

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

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
