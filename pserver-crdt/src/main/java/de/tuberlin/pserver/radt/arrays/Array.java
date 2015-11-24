package de.tuberlin.pserver.radt.arrays;

import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.*;

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
    public T[] getLocalCopy() {
        List<T> result = new LinkedList<>();

        for (Item<T> item : array) {
            result.add(item.getValue());
        }

        return (T[])result.toArray();
    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        ArrayOperation<Item<T>> radtOp = (ArrayOperation<Item<T>>) op;

        if(radtOp.getType() == Operation.WRITE) {
            return remoteWrite(radtOp.getValue());
        }
        else {
            throw new IllegalArgumentException("Array RADTs do not allow the " + op.getOperationType() + " operation.");
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private T localRead(int index) {
        return array[index].getValue();
    }

    private boolean localWrite(int index, T value) {
        int[] clock = increaseVectorClock();
        S4Vector s4 = new S4Vector(sessionID, nodeId, clock, 0);
        Item<T> item = new Item<>(index, clock, new S4Vector(sessionID, nodeId, clock, 0), value);
        array[index] = item;

        broadcast(new ArrayOperation<>(Operation.WRITE, item, index, clock, s4));

        return true;
    }

    private boolean remoteWrite(Item<T> item) {
        Item old = array[item.getIndex()];
        if(old.getS4Vector().takesPrecedenceOver(item.getS4Vector())) {
            array[item.getIndex()] = item;
            return true;
        }

        return false;
    }
}
