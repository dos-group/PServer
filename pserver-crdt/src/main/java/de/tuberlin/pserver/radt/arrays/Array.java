package de.tuberlin.pserver.radt.arrays;

import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.radt.CObject;
import de.tuberlin.pserver.radt.Item;
import de.tuberlin.pserver.radt.RADTOperation;
import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.runtime.RuntimeManager;

import java.util.*;

public class Array<T> extends AbstractArray<T> implements IArray<T>{

    /**
     * Sole constructor
     *
     * @param id             the ID of the CRDT that this replica belongs to
     * @param runtimeManager the {@code RuntimeManager} belonging to this {@code MLProgram}
     */
    public Array(int size, String id, int noOfReplicas, RuntimeManager runtimeManager) {
        super(size, id, noOfReplicas, runtimeManager);
    }

    @Override
    public T read(int index) {
        return localRead(index);
    }

    @Override
    public boolean write(int index, T value) {
        return localWrite(index, value);
    }

    private T localRead(int index) {
        return array[index].getValue();
    }

    private boolean localWrite(int index, T value) {

        int[] clock = increaseVectorClock();
        Item<T> item = new Item<>(index, clock, new S4Vector(sessionID, siteID, clock, 0), value);
        array[index] = item;

        RADTOperation<CObject<T>> blub = new RADTOperation<CObject<T>>(Operation.WRITE, item, index, clock, new S4Vector(sessionID, siteID, clock, 0));
        Operation<CObject<T>> blah = (Operation<CObject<T>>) blub;
        broadcast(blub);

        return true;
    }

    private boolean remoteWrite(Item<T> item) {
        //updateVectorClock(item.getVectorClock());
        Item old = array[item.getIndex()];
        if(old.getS4Vector().takesPrecedenceOver(item.getS4Vector())) {
            array[item.getIndex()] = item;
            return true;
        }

        return false;

    }

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        RADTOperation<Item<T>> radtOp = (RADTOperation<Item<T>>) op;

        if(radtOp.getType() == Operation.WRITE) {
            return remoteWrite(radtOp.getValue());
        }
        else {
            // TODO: exception text
            throw new UnsupportedOperationException();
        }
    }

    public T[] getArray() {
        List<T> result = new LinkedList<>();

        for (Item<T> anArray : array) {
            result.add(anArray.getValue());
        }

        return (T[])result.toArray();
    }

    public Queue getQueue() {
        return this.queue;
    }
}
