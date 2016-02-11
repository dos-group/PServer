package de.tuberlin.pserver.radt.list;

import de.tuberlin.pserver.radt.AbstractCemetery;
import de.tuberlin.pserver.radt.hashtable.Slot;

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
        int i = 0;
        Node<T> node;

        for(Queue<Node<T>> queue : cemetery) {
            //System.out.println("[DEBUG]" + nodeId +" Checking cemetery queue " + i);
            i++;


            while(allReplicasHaveExecutedDelete(queue.peek())) {
                node = queue.poll();
                //System.out.println("[DEBUG]" + nodeId +" Removing " + node.getS4Vector());
                list.removeTombstone(node);
                purged = true;
            }
        }
        return purged;
    }

    private synchronized boolean allReplicasHaveExecutedDelete(Node<T> node) {
        // Every site has executed the deletion D, therefore from now on only operations happening after D will arrive
        if(node == null) return false;
        //System.out.println("Seq: " + node.getS4Vector().getSeq());
        //System.out.println("Min: " + getMinVectorClockEntry(node.getS4Vector().getSiteId())+"\n");
        return node.getUpdateDeleteS4().getSeq() <= getMinVectorClockEntry(node.getUpdateDeleteS4().getSiteId());
    }

    private synchronized boolean noTwo(Node<T> node) {
        if(node.getLink() == null) return true;
        // TODO: should I use node.getUpdateDeleteS4().getSiteId() ? => prob no
        return node.getLink().getS4Vector().getVectorClockSum() < getMinVectorSumEntry();
    }
}
