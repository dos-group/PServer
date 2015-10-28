package de.tuberlin.pserver.radt.arrays;

import de.tuberlin.pserver.crdt.operations.Operation;
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
        array[index] = new Item<>(index, clock, new S4Vector(sessionID, siteID, clock, 0), value);

        broadcast(new RADTOperation<>(Operation.WRITE, value, index, clock, new S4Vector(sessionID, siteID, clock, 0)));

        return true;
    }

    private boolean remoteWrite(int index, int[] remoteVectorClock, T value, S4Vector s4) {
        queue.add(new Item<>(index, remoteVectorClock, s4, value));

        boolean wrote = false;

        while(queue.peek() != null && isCausallyReadyFor(queue.peek())) {
            Item newObj = queue.poll();
            Item old = array[newObj.getIndex()];
            updateVectorClock(newObj.getVectorClock());

            if(old.getS4Vector().takesPrecedenceOver(newObj.getS4Vector())) {
                array[newObj.getIndex()] = newObj;
                wrote = true;
            }
        }
        return wrote;
    }

    private void updateVectorClock(int[] remoteVectorClock) {
        for(int i = 0; i < remoteVectorClock.length; i++) {
            vectorClock[i] = Math.max(vectorClock[i], remoteVectorClock[i]);
        }
    }

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        RADTOperation<T> radtOp = (RADTOperation<T>) op;

        if(radtOp.getType() == Operation.WRITE) {
            return remoteWrite(radtOp.getIndex(), radtOp.getVectorClock(), radtOp.getValue(), radtOp.getS4Vector());
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
}
