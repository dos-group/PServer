package de.tuberlin.pserver.radt.arrays;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.operations.Operation;
import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.LinkedList;
import java.util.List;

public class Array<T> extends AbstractArray<T> implements IArray<T>{

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

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

        Preconditions.checkState(!isFinished, "After finish() has been called on an RADT no more changes can be made to it");

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
        ArrayOperation<T> arrayOp = (ArrayOperation<T>) op;

        switch(arrayOp.getType()) {

            case WRITE:

                return remoteWrite(arrayOp.getValue(), arrayOp.getIndex(), arrayOp.getS4Vector());

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

        Element<T> item = new Element<>(value, s4);

        set(index, item);

        broadcast(new ArrayOperation<>(Operation.OpType.WRITE, value, index, clock, s4));

        return true;

    }

    private synchronized boolean remoteWrite(T value, int index, S4Vector s4) { //Element<T> item) {

        Element current = get(index);

        if(current.getS4Vector().precedes(s4)) {

            set(index, new Element<>(value, s4));

            return true;

        }

        return false;

    }

}
