package de.tuberlin.pserver.radt.arrays;

import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.radt.list.Node;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.LinkedList;
import java.util.List;

public class Array<T> extends AbstractArray<T> implements IArray<T>{

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    /**
     * Sole constructor
     *
     * @param id             the ID of the CRDT that this replica belongs to
     * @param programContext the {@code RuntimeManager} belonging to this {@code MLProgram}
     */
    public Array(int size, String id, int noOfReplicas, ProgramContext programContext) {
        super(size, id, noOfReplicas, programContext);
        ready();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public T read(int index) {
        return localRead(index);
    }

    @Override
    public boolean write(int index, T value) {
        return localWrite(index, value);
    }

    @Override
    public synchronized Object[] getArray() {
        List<T> result = new LinkedList<>();

        for (int i = 0; i < size; i++) {
            result.add(get(i).getValue());
        }

        return result.toArray();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Array[");

        for(int i = 0; i < size; i++) {
            sb.append(get(i).getValue());
            if (i < size - 1) sb.append(", ");
        }
        sb.append("]");

        return sb.toString();
    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        @SuppressWarnings("unchecked")
        ArrayOperation<Item<T>> arrayOp = (ArrayOperation<Item<T>>) op;

        switch(arrayOp.getType()) {
            case WRITE:
                return remoteWrite(arrayOp.getValue());
            default:
                throw new IllegalArgumentException("Array RADTs do not allow the " + op.getType() + " operation.");
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private synchronized T localRead(int index) {
        return get(index).getValue();
    }

    private synchronized boolean localWrite(int index, T value) {
        int[] clock = increaseVectorClock();
        S4Vector s4 = new S4Vector(nodeId, clock);

        Item<T> item = new Item<>(index, clock, s4, value);
        set(index, item);

        broadcast(new ArrayOperation<>(Operation.OpType.WRITE, item, index, clock, s4));

        return true;
    }

    private synchronized boolean remoteWrite(Item<T> item) {
        Item current = get(item.getIndex());

        if(current.getS4Vector().takesPrecedenceOver(item.getS4Vector())) {
            set(item.getIndex(), item);
            return true;
        }

        return false;
    }
}
