package de.tuberlin.pserver.crdt.radt;

import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.runtime.RuntimeManager;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

public abstract class AbstractRADT<T> extends AbstractCRDT<T> implements RADT<T> {
    protected final int[] vectorClock;
    protected final int siteID;
    // priority queue
    protected final Queue<CObject<T>> queue;
    // TODO: Not sure what this does...
    protected int sessionID;
    protected final int size;


    protected AbstractRADT(int size, String id, int noOfReplicas, RuntimeManager runtimeManager) {
        super(id, noOfReplicas, runtimeManager);

        // Initialize size
        this.size = size;

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

        // Initialize session ID
        // TODO: what is this for?
        this.sessionID = 0;
    }

    // TODO: this method is stupid, there should be a public getNodeID in runtimeManager
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

    protected boolean isCausallyReadyFor(CObject cOb) {
        // TODO: what about if sum > vectorClockSum => must the operation be purged from queue?
        // TODO: this needs verification
        System.out.println();
        System.out.println("Local vector clock: " + vectorClock[cOb.getS4Vector().getSiteId()]);
        System.out.println("Remote vector clock: " + cOb.getVectorClock()[cOb.getS4Vector().getSiteId()]);
        return vectorClock[cOb.getS4Vector().getSiteId()] == (cOb.getVectorClock()[cOb.getS4Vector().getSiteId()] - 1);
    }

    protected int[] increaseVectorClock(int id) {
        vectorClock[id]++;
        return vectorClock.clone();
    }
}
