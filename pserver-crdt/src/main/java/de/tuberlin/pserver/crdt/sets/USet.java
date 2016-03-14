package de.tuberlin.pserver.crdt.sets;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.ReplicatedDataTypeException;
import de.tuberlin.pserver.operations.Operation;
import de.tuberlin.pserver.operations.SimpleOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.HashSet;
import java.util.Set;

/**
 * The Unique Set assumes each value inserted into the set is unique. Hence, there is no need for a tombstone set.
 * However, add must always be delivered before remove!
 */

public class USet<T> extends AbstractSet<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Set<T> set;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public USet(String id, int noOfReplicas, ProgramContext programContext) {

        super(id, noOfReplicas, programContext);
        

        this.set = new HashSet<>();

        ready();

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public boolean add(T value) {

        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

        if(addElement(value)) {

            broadcast(new SimpleOperation<>(Operation.OpType.ADD, value));

            return true;

        }

        return false;

    }

    @Override
    public boolean remove(T value) {

        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

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

                throw new IllegalArgumentException("USet CRDTs do not allow the " + op.getType() + " operation.");

        }

    }


    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private synchronized boolean addElement(T value) {

        if(!set.add(value)) {

            throw new ReplicatedDataTypeException("The value "+ value + " is already contained in the Unique Set and cannot be " +
                    "added again.");

        }

        return true;

    }

    private synchronized boolean removeElement(T value) {

        return set.remove(value);

    }

}
