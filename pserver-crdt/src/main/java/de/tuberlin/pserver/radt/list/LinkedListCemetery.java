package de.tuberlin.pserver.radt.list;

import de.tuberlin.pserver.radt.AbstractCemetery;

import java.util.*;

public class LinkedListCemetery<T> extends AbstractCemetery<Node<T>> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final LinkedList<T> list;

    // One queue per remote replica
    private final Map<Integer,Queue<Node<T>>> tombstoneQueues;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LinkedListCemetery(LinkedList<T> list, int noOfReplicas, int nodeId) {

        super(noOfReplicas, nodeId);

        this.list = list;

        this.tombstoneQueues = new HashMap<>();

        for(int i = 0; i < noOfReplicas; i++) {

            if(i != nodeId) {

                tombstoneQueues.put(i, new java.util.LinkedList<Node<T>>());

            }

        }

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public synchronized boolean purge() {

        boolean purged = false;

        Node<T> node;

        for(Queue<Node<T>> queue : cemetery) {

            while(allReplicasHaveExecutedDelete(queue.peek()) && doesRightCobjectSucceed(queue.peek())) {
                node = queue.poll();

                list.removeTombstone(node);

                purged = true;

            }

        }

        return purged;

    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private synchronized boolean allReplicasHaveExecutedDelete(Node<T> node) {

        // Every site has executed the deletion D, therefore from now on only operations happening after D will arrive
        if(node == null) return false;

        return node.getUpdateDeleteS4().getSeq() <= getMinVectorClockEntry(node.getUpdateDeleteS4().getSiteId());

    }

    private synchronized boolean doesRightCobjectSucceed(Node<T> node) {
        if(node == null) return false;

        if(node.getLink() == null) return true;

        return node.getLink().getS4Vector().getVectorClockSum() < getMinVectorSumEntry();

    }

}
