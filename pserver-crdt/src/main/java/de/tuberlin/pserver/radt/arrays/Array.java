package de.tuberlin.pserver.radt.arrays;

import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.radt.RADTOperation;
import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.runtime.RuntimeManager;

import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;

public class Array<T> extends AbstractCRDT<T> {
    private final class CObject<T> {
        private final int index;
        private final int[] vectorClock;
        private final S4Vector s4Vector;
        private final T value;

        public CObject(int index, int[] vectorClock, S4Vector s4Vector, T value) {
            this.index = index;
            this.vectorClock = vectorClock;
            this.s4Vector = s4Vector;
            this.value = value;
        }

        public int[] getVectorClock() {
            return vectorClock;
        }

        public S4Vector getS4Vector() {
            return s4Vector;
        }

        public T getValue() {
            return value;
        }

        public int getIndex() {
            return index;
        }
    }

    private final int[] vectorClock;
    private final int siteID;
    //priority queue
    private final Queue<CObject<T>> queue;
    private final CObject<T>[] array;
    // Not sure what this does...
    private int sessionID;

    /**
     * Sole constructor
     *
     * @param id             the ID of the CRDT that this replica belongs to
     * @param runtimeManager the {@code RuntimeManager} belonging to this {@code MLProgram}
     */
    protected Array(int size, String id, int noOfReplicas, RuntimeManager runtimeManager) {
        super(id, noOfReplicas, runtimeManager);

        // Initialize vector clock
        this.vectorClock = new int[runtimeManager.getNodeIDs().length];
        Arrays.fill(vectorClock, 0);

        // Initialize site ID
        // TODO: we need a getNodeID() function in runtimeManager
        this.siteID = getNodeID(runtimeManager);

        // Initialize queue
        this.queue = new PriorityQueue<>(new Comparator<CObject>() {
            @Override
            public int compare(CObject o1, CObject o2) {
                if (o1.getS4Vector().takesPrecedenceOver(o2.getS4Vector())) {
                    return -1;
                } else if (o2.getS4Vector().takesPrecedenceOver(o1.getS4Vector())) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });

        // Initialize array
        array = new CObject[size];
        Arrays.fill(array, new CObject(-1, vectorClock, new S4Vector(0, 0, new int[0], 0), null));

        // Initialize session ID
        // TODO: what is this for?
        this.sessionID = 0;


    }

    private int getNodeID(RuntimeManager runtimeManager) {
        int[] a = runtimeManager.getNodeIDs();
        int[] b = runtimeManager.getRemoteNodeIDs();

        for (int anA : a) {
            boolean found = false;
            for (int aB : b) {
                if (anA == aB) {
                    found = true;
                }
                if (!found) {
                    return anA;
                }
            }
        }
        return -1;
    }

    public T read(int index) {
        return localRead(index);
    }

    public boolean write(int index, T value) {
        return localWrite(index, value);
    }

    private T localRead(int index) {
        return array[index].getValue();
    }

    private boolean localWrite(int index, T value) {

        int[] blub = increaseVectorClock(siteID);
        array[index] = new CObject<>(index, blub, new S4Vector(sessionID, siteID, blub, 0), value);

        broadcast(new RADTOperation<>(Operation.WRITE, value, index, blub, new S4Vector(sessionID, siteID, blub, 0)));

        return true;
    }

    private boolean remoteWrite(int index, int[] remoteVectorClock, T value, S4Vector s4) {
        queue.add(new CObject<>(index, remoteVectorClock, s4, value));

        boolean wrote = false;

        while(queue.peek() != null && isCausallyReady(queue.peek())) {
            CObject newObj = queue.poll();
            CObject old = array[newObj.getIndex()];
            setNewVectorClock(newObj.getVectorClock());

            if(old.getS4Vector().takesPrecedenceOver(newObj.getS4Vector())) {
                array[newObj.getIndex()] = newObj;
                wrote = true;
            }
        }
        return wrote;
    }

    private int[] increaseVectorClock(int id) {
        vectorClock[id]++;
        return vectorClock.clone();
    }

    private void decreaseVectorClock(int id) {
        vectorClock[id] = vectorClock[id] - 1;
    }

    private void setNewVectorClock(int[] remoteVectorClock) {
        for(int i = 0; i < remoteVectorClock.length; i++) {
            vectorClock[i] = Math.max(vectorClock[i], remoteVectorClock[i]);
        }
    }

    private boolean isCausallyReady(CObject cOb) {
        // TODO: what about if sum > vectorClockSum => must the operation be purged from queue?
        // TODO: this needs verification
        System.out.println();
        System.out.println("Local vector clock: " + vectorClock[cOb.getS4Vector().getSiteId()]);
        System.out.println("Remote vector clock: " + cOb.getVectorClock()[cOb.getS4Vector().getSiteId()]);
        return vectorClock[cOb.getS4Vector().getSiteId()] == (cOb.getVectorClock()[cOb.getS4Vector().getSiteId()] - 1);
    }

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        RADTOperation<T> radtOp = (RADTOperation<T>) op;

        if(radtOp.getType() == Operation.WRITE) {
            return remoteWrite(radtOp.getIndex(), radtOp.getVectorClock(), radtOp.getValue(), radtOp.getS4());
        }
        else {
            // TODO: exception text
            throw new UnsupportedOperationException();
        }
    }

    public T[] getArray() {
        List<T> result = new LinkedList<T>();

        for(int i = 0; i < array.length; i++) {
            result.add(array[i].getValue());
        }

        return (T[])result.toArray();
    }
}
