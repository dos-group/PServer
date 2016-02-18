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
        //System.out.println("*enrol*");
        cObj.makeTombstone();
        return cemetery.get(nodeId).add(cObj);
    }

    // TODO: improve this, don't really want to have to check every queue...
    @Override
    public synchronized boolean withdraw(T cObj) {
        boolean withdrawn = false;

        for(Queue queue : cemetery) {
            if(queue.contains(cObj)) {
                queue.remove(cObj);
                withdrawn = true;
                break;
            }
        }

        return withdrawn;
    }

    protected synchronized int getMinVectorClockEntry(int siteId) {
        int min = Integer.MAX_VALUE;

        for(int i = 0; i < lastVectorClocks.length; i++) {
            // TODO: use a map instead of nodeId != null ?
            if(i != nodeId) {
                //System.out.println(lastVectorClocks[i][0] + ", " + lastVectorClocks[i][1]);
                if (lastVectorClocks[i][siteId] < min) {
                    min = lastVectorClocks[i][siteId];
                }
            }
        }

        return min;
    }

    protected synchronized int getMinVectorSumEntry() {
        int min = Integer.MAX_VALUE;
        int curr;

        for(int i = 0; i < lastVectorClocks.length; i++) {
            if(i != nodeId) {
                curr = Arrays.stream(lastVectorClocks[i]).sum();
                if(curr < min) {
                    min = curr;
                }
            }
        }

        return min;
    }

    public void updateAndPurge(int[] vectorClock, int srcNode) {
        lastVectorClocks[srcNode] = vectorClock;
        // TODO: when, where and how often to purge?
        purge();
    }
}
