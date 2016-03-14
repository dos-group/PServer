package de.tuberlin.pserver.crdt.sets;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.operations.Operation;
import de.tuberlin.pserver.operations.SimpleOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * Maintain a counter for each element. The element is in the set if its count is &gt; 0.
 */
public class PNSet<T> extends AbstractSet<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Map<T, Integer> counter;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PNSet(String id, int noOfReplicas, ProgramContext programContext) {

        super(id, noOfReplicas, programContext);

        counter = new HashMap<>();

        ready();

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public boolean add(T element) {

        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

        if(addElement(element)) {

            broadcast(new SimpleOperation<>(Operation.OpType.ADD, element));

            return true;

        }

        return false;

    }

    @Override
    public boolean remove(T element) {

        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

        if(removeElement(element)) {

            broadcast(new SimpleOperation<>(Operation.OpType.REMOVE, element));

            return true;

        }

        return false;

    }

    @Override
    public synchronized Set<T> getSet() {

        return counter.keySet().stream()
                .filter(key -> counter.get(key) > 0)
                .collect(Collectors.toSet());

    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    @Override
    protected boolean update(int srcNodeId, Operation op) {

        @SuppressWarnings("unchecked")
        SimpleOperation<T> simpleOp = (SimpleOperation<T>) op;

        switch(simpleOp.getType()) {

            case ADD:

                return addElement(simpleOp.getValue());

            case REMOVE:

                return removeElement(simpleOp.getValue());

            default:

                throw new IllegalArgumentException("PNSet CRDTs do not allow the " + op.getType() + " operation.");

        }

    }

    private synchronized boolean addElement(T element) {

        counter.putIfAbsent(element, 0);

        counter.compute(element, (key, val) -> ++val);

        return true;

    }

    private synchronized boolean removeElement(T element) {

        counter.putIfAbsent(element, 0);

        counter.computeIfPresent(element, (key, val) -> --val);

        return true;

    }

}