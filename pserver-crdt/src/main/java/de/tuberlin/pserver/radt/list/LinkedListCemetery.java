package de.tuberlin.pserver.radt.list;

import de.tuberlin.pserver.radt.AbstractCemetery;

import java.util.*;

public class LinkedListCemetery<T> extends AbstractCemetery<Node<T>> {
    private final LinkedList<T> list;

    // One queue per remote replica
    private final Map<Integer,Queue<Node<T>>> tombstoneQueues;


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

    public synchronized boolean purge() {
        boolean purged = false;
        Node<T> node;

        for(Queue<Node<T>> queue : cemetery) {

            //System.out.println(nodeId + "One: " + allReplicasHaveExecutedDelete(queue.peek()) + ", Two: " + doesRightCobjectSucceed(queue.peek()));

            while(allReplicasHaveExecutedDelete(queue.peek()) && doesRightCobjectSucceed(queue.peek())) {
                node = queue.poll();
                //System.out.println(list);
                System.out.println("[DEBUG] " + nodeId +" Purging " + node.getS4Vector() + " | " + node.getUpdateDeleteS4());
                list.removeTombstone(node);
                //System.out.println(list);

                purged = true;
            }
        }
        return purged;
    }

    private synchronized boolean allReplicasHaveExecutedDelete(Node<T> node) {
        // Every site has executed the deletion D, therefore from now on only operations happening after D will arrive
        if(node == null) return false;
        System.out.println("\n" + "Seq: " + node.getUpdateDeleteS4().getSeq());
        System.out.println("Min: " + getMinVectorClockEntry(node.getUpdateDeleteS4().getSiteId()));
        return node.getUpdateDeleteS4().getSeq() <= getMinVectorClockEntry(node.getUpdateDeleteS4().getSiteId());
    }

    private synchronized boolean doesRightCobjectSucceed(Node<T> node) {
        if(node == null) return false;
        if(node.getLink() == null) return true;
        // TODO: should I use node.getUpdateDeleteS4().getSiteId() ? => prob no
        //System.out.println("Sum: " + node.getLink().getS4Vector().getVectorClockSum());
        //System.out.println("Min sum: " + getMinVectorSumEntry()+"\n");
        return node.getLink().getS4Vector().getVectorClockSum() < getMinVectorSumEntry();
    }
}
