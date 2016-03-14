package de.tuberlin.pserver.radt;

import java.util.*;
import java.util.stream.IntStream;

public abstract class AbstractCemetery<T extends CObject> implements Cemetery<T> {


    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int[][] lastVectorClocks;

    protected final List<Queue<T>> cemetery;

    protected final int nodeId;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractCemetery(int noOfReplicas, int nodeId) {

        this.cemetery = new ArrayList<>();

        IntStream.range(0,noOfReplicas).forEach(i -> cemetery.add(new LinkedList<>()));

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

        cObj.makeTombstone();

        return cemetery.get(nodeId).add(cObj);

    }

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

    public synchronized void updateAndPurge(int[] localVectorClock, int[] remoteVectorClock, int srcNode) {

        lastVectorClocks[nodeId] = localVectorClock;

        lastVectorClocks[srcNode] = remoteVectorClock;

        purge();

    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    protected synchronized int getMinVectorClockEntry(int siteId) {

        int min = Integer.MAX_VALUE;

        for(int i = 0; i < lastVectorClocks.length; i++) {

            if (lastVectorClocks[i][siteId] < min) {

                min = lastVectorClocks[i][siteId];

            }

        }

        return min;
    }

    protected synchronized int getMinVectorSumEntry() {

        int min = Integer.MAX_VALUE;

        int curr;

        for(int i = 0; i < lastVectorClocks.length; i++) {

            curr = Arrays.stream(lastVectorClocks[i]).sum();

            if(curr < min) {

                min = curr;

            }

        }

        return min;

    }

}
