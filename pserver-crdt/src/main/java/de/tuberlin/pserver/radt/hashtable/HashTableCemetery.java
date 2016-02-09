package de.tuberlin.pserver.radt.hashtable;

import de.tuberlin.pserver.radt.AbstractCemetery;

import java.util.*;

public class HashTableCemetery<K,V> extends AbstractCemetery<Slot<K,V>> {
    private final Hashtable<K,Slot<K,V>> hashTable;
    private final Map<Integer,Queue<Slot<K,V>>> tombstoneQueues;


    public HashTableCemetery(Hashtable<K,Slot<K,V>> hashTable, int noOfReplicas, int nodeId) {
        super(noOfReplicas, nodeId);
        this.hashTable = hashTable;
        this.tombstoneQueues = new HashMap<>();

        for(int i = 0; i < noOfReplicas; i++) {
            if(i != nodeId) {
                tombstoneQueues.put(i, new LinkedList<>());
            }
        }
    }

    public synchronized boolean purge() {
        //System.out.println("*PURGE*");
        //System.out.println("Table: " + hashTable);

        boolean purged = false;
        for(int i = 0; i < cemetery.size(); i++) {
        //for(Slot<K,V> slot : new ArrayList<>(cemetery)) {
            Queue<Slot<K,V>> queue = cemetery.get(i);
            while(conditionOne(queue.peek())) {  // Condition is true so slot should be deleted
                Slot<K,V> slot = queue.poll();

                System.out.println("*TRUE*");
                K key = slot.getKey();
                Slot<K,V> currSlot = hashTable.get(slot.getKey());
                System.out.println("Slot: " + slot);
                System.out.println("Slot key: " + slot.getKey());
                System.out.println("Curr: " + currSlot);
                Slot<K,V> prevSlot = null;

                // If slot is the first with this key
                if(slot.equals(currSlot)) {
                    // If slot is the only one with this key, remove the key from hashtable
                    if(!currSlot.hasNextSlot()) {
                        hashTable.remove(key);
                        withdraw(i, slot);
                        purged = true;
                    }
                    // Else set the next slot as the head of the chain
                    else {
                        hashTable.put(key, currSlot.getNextSlot());
                        withdraw(i, slot);
                        purged = true;
                    }
                }

                // If slot is not the first in the chain of slots with the same key (or colliding keys)
                else {
                    prevSlot = currSlot;
                    currSlot = currSlot.getNextSlot();
                    boolean found = false;

                    while(!found) {
                        if(slot.equals(currSlot)) {
                            found = true;
                            prevSlot.setNextSlot(currSlot.getNextSlot());
                            withdraw(i, slot);
                            purged = true;

                        }

                        prevSlot = currSlot;
                        currSlot = currSlot.getNextSlot();
                    }
                }
            }
        }
        return purged;
    }
}
