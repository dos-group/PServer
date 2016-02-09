package de.tuberlin.pserver.radt;

import de.tuberlin.pserver.radt.CObject;
import de.tuberlin.pserver.radt.hashtable.HashTable;
import de.tuberlin.pserver.radt.hashtable.Slot;

import java.util.*;
import java.util.stream.IntStream;

// TODO: Cemetery<T extends CObject>
public abstract class AbstractCemetery<T extends CObject> implements Cemetery<T> {
    private final int[][] lastVectorClocks;

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    // TODO: FIFO queue?
    protected final List<Queue<T>> cemetery;
    protected final int nodeId;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractCemetery(int noOfReplicas, int nodeId) {
        this.cemetery = new ArrayList<>();

        IntStream.range(0,noOfReplicas).forEach(i -> cemetery.add(new LinkedList<>()));

        /*for(int i = 0; i < noOfReplicas; i++) {
            cemetery.add(new LinkedList<>());
        }*/

        this.nodeId = nodeId;
        this.lastVectorClocks = new int[noOfReplicas][noOfReplicas];

        for (int[] vectorClock : lastVectorClocks) {
            Arrays.fill(vectorClock, 0);
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public synchronized boolean enrol(int nodeId, T cObj) {
        System.out.println("*enrol*");
        return cemetery.get(nodeId).add(cObj);
    }

    @Override
    public synchronized boolean withdraw(int nodeId, T cObj) {
        return cemetery.get(nodeId).remove(cObj);
    }

    protected synchronized boolean conditionOne(CObject obj) {
        // Every site has executed the deletion D, therefore from now on only operations happening after D will arrive
        //System.out.println("Seq: " + obj.getS4Vector().getSeq());
        //System.out.println("Min: " + getMinVectorClockEntry(obj.getS4Vector().getSiteId())+"\n");
        if(obj == null) return false;
        return obj.getS4Vector().getSeq() <= getMinVectorClockEntry(obj.getS4Vector().getSiteId());
    }

    protected synchronized boolean conditionTwo(CObject obj) {
        return false;
    }

    protected synchronized int getMinVectorClockEntry(int siteId) {
        int min = Integer.MAX_VALUE;

        for(int i = 0; i < lastVectorClocks.length; i++) {
            if(i != nodeId) {
                System.out.println(lastVectorClocks[i][siteId]);
                if (lastVectorClocks[i][siteId] < min) {
                    min = lastVectorClocks[i][siteId];
                }
            }
        }

        return min;
    }

    public void updateVectorClocks(int srcNode, int[] vectorClock) {
        lastVectorClocks[srcNode] = vectorClock;
        // TODO: when, where and how often to purge?
        purge();
    }
}
