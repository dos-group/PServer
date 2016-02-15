package de.tuberlin.pserver.radt.hashtable;

import de.tuberlin.pserver.radt.AbstractCemetery;
import de.tuberlin.pserver.radt.CObject;

import java.util.*;

public class HashTableCemetery<K,V> extends AbstractCemetery<Slot<K,V>> {
    private final Map<K,Slot<K,V>> map;
    // One queue per remote replica
    private final Map<Integer,Queue<Slot<K,V>>> tombstoneQueues;


    public HashTableCemetery(Map<K, Slot<K,V>> map, int noOfReplicas, int nodeId) {
        super(noOfReplicas, nodeId);

        this.map = map;
        this.tombstoneQueues = new HashMap<>();

        for(int i = 0; i < noOfReplicas; i++) {
            if(i != nodeId) {
                tombstoneQueues.put(i, new LinkedList<>());
            }
        }
    }

    public synchronized boolean purge() {
        boolean purged = false;

        for(Queue<Slot<K, V>> queue : cemetery) {

            while (allReplicasHaveExecutedDelete(queue.peek())) {
                //System.out.println("[DEBUG]" + nodeId +" Removing " + slot.getKey());
                Slot<K, V> slot = queue.poll();
                map.remove(slot.getKey());
                purged = true;
            }
        }
        return purged;
    }

    private synchronized boolean allReplicasHaveExecutedDelete(Slot<K,V> slot) {
        // Every site has executed the deletion D, therefore from now on only operations happening after D will arrive
        if(slot == null) return false;
        //System.out.println("Seq: " + slot.getS4Vector().getSeq());
        //System.out.println("Min: " + getMinVectorClockEntry(slot.getS4Vector().getSiteId())+"\n");
        return slot.getS4Vector().getSeq() <= getMinVectorClockEntry(slot.getS4Vector().getSiteId());
    }
}
