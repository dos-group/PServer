package de.tuberlin.pserver.crdt.sets;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.operations.Operation;
import de.tuberlin.pserver.operations.TaggedOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.*;
import java.util.Set;

public class ORSet<T> extends AbstractSet<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Map<T,List<UUID>> map = new HashMap<>();

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ORSet(String id, int noOfReplicas, ProgramContext programContext) {

        super(id, noOfReplicas, programContext);

        ready();

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public boolean add(T value) {

        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

        UUID id = UUID.randomUUID();

        if(addElement(value, id)) {

            broadcast(new TaggedOperation<>(Operation.OpType.ADD, value, id));

            return true;

        }

        return false;

    }

    @Override
    public synchronized boolean remove(T value) {

        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

        List<UUID> idList = getIds(value);

        if(idList.size() == 0) return false;

        if(removeElement(value, idList)) {

            broadcast(new TaggedOperation<>(Operation.OpType.REMOVE, value, idList));

            return true;

        }

        return false;

    }

    @Override
    public synchronized Set<T> getSet() {

        return map.keySet();

    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    @Override
    protected boolean update(int srcNodeId, Operation op) {

        switch(op.getType()) {

            case ADD:

                @SuppressWarnings("unchecked")
                TaggedOperation<T,UUID> taggedOp = (TaggedOperation<T,UUID>) op;

                return addElement(taggedOp.getValue(), taggedOp.getTag());

            case REMOVE:

                @SuppressWarnings("unchecked")
                TaggedOperation<T,List<UUID>> listTaggedOp = (TaggedOperation<T,List<UUID>>) op;

                return removeElement(listTaggedOp.getValue(), listTaggedOp.getTag());

            default:

                throw new IllegalArgumentException("ORSet CRDTs do not allow the " + op.getType() + " operation.");

        }

    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private synchronized boolean addElement(T value, UUID id) {


        map.putIfAbsent(value, new ArrayList<>());

        map.get(value).add(id);

        return true;

    }

    private synchronized boolean removeElement(T value, List<UUID> id) {

        boolean removed = map.get(value).removeAll(id);

        if(map.get(value).size() == 0) { map.remove(value);}

        return removed;

    }

    private synchronized List<UUID> getIds(T value) {

        if(map.containsKey(value)) {

            return new ArrayList<>(map.get(value));

        }

        return new ArrayList<>();

    }

}