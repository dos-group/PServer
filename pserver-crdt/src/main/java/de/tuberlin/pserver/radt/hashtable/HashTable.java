package de.tuberlin.pserver.radt.hashtable;

import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.radt.CObject;
import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.radt.Slot;
import de.tuberlin.pserver.runtime.RuntimeManager;

public class HashTable<K,V> extends AbstractHashTable<K,V> {
    private Cemetery<Slot<K,V>> cemetery;

    public HashTable(int size, String id, int noOfReplicas, RuntimeManager runtimeManager) {
        super(size, id, noOfReplicas, runtimeManager);
        cemetery = new Cemetery<>();
    }

    @Override
    protected boolean update(int srcNodeId, Operation<?> op) {
        HashTableOperation<K,Slot<K,V>> radtOp = (HashTableOperation<K,Slot<K,V>>) op;

        if(radtOp.getType() == Operation.PUT) {
           // System.out.println("Received PUT: " + radtOp.getValue());
            return remotePut(radtOp.getKey(), radtOp.getValue().getValue(), radtOp.getS4Vector());
        }
        else if(radtOp.getType() == Operation.REMOVE) {
            return remoteRemove(radtOp.getKey(), radtOp.getS4Vector());
        }
        else {
            // TODO: exception message
            throw new UnsupportedOperationException("blub");
        }
    }

    public void put(K key, V value) {
        int[] clock = increaseVectorClock();

        S4Vector s4 = new S4Vector(sessionID, siteID, clock, 0);

        Slot<K,V> slot = localPut(key, value, s4, clock);

        // TODO: index is not needed here?
        if(slot != null) {
            broadcast(new HashTableOperation<>(Operation.PUT, key, slot, 0, clock, s4));
        }
        else {
            // Todo: throw exception?
            System.out.println("FAIL");
        }
        /*System.out.println("Local put " + value + " at " + siteID + " with Vectorclock: <" + s4.getSessionNumber() +
                ", "+ s4.getSiteId() + ", " + s4.getVectorClockSum() + ", " + s4.getSeq() +">");
        System.out.println(this);*/
    }

    public V read(K key) {
        if(key == null) return null;

        Slot<K,V> slot = hashTable.get(key);

        if(slot == null || slot.isTombstone()) {
            return null;
        }

        return slot.getValue();
    }

    public boolean remove(K key) {
        Slot<K,V> slot = localRemove(key);

        if(slot != null) {
            int[] clock = increaseVectorClock();
            S4Vector s4 = new S4Vector(sessionID, siteID, clock, 0);
            //Slot<K,V> slot = new Slot<>(key, null, s4, null, clock);

            broadcast(new HashTableOperation<>(Operation.REMOVE, key, slot, 0, clock, s4));
            return true;
        }
        else {
            return false;
        }
    }

    private Slot<K,V> localPut(K key, V value, S4Vector s4, int[] clock) {
        // TODO: what about collisions?
        Slot<K,V> slot = hashTable.get(key);

        if(slot != null) {
            slot = new Slot(key, value, s4, slot.getNextSlot(), clock);
            hashTable.put(key, slot);
            return slot;
        } else {
            slot = new Slot(key, value, s4, null, clock);
            hashTable.put(key, slot);
            return slot;
        }
    }

    private Slot<K,V> localRemove(K key) {
        Slot<K,V> slot = hashTable.get(key);

        if(slot == null){
            return slot;
        }
        else {
            slot.setValue(null);
            return slot;
        }
    }

    private boolean remotePut(K key, V value, S4Vector s4) {
        Slot<K,V> previous = null;
        Slot<K,V> slot = hashTable.get(key);

        // Chaining in case of collisions
        while(slot != null && !key.equals(slot.getKey())) {
            previous = slot;
            slot = slot.getNextSlot();
        }



        if(slot != null && s4.takesPrecedenceOver(slot.getS4Vector())) {
            return false;
        }
        else if (slot != null && slot.isTombstone()) { // slot is a tombstone
            cemetery.withdraw(slot);
        }
        else if(slot == null) {
            slot = new Slot<>(key, value, s4, null, null);
            if(previous != null) {
                previous.setNextSlot(slot);
            }
            hashTable.put(key, slot);
        }

        slot.setValue(value);
        slot.setS4Vector(s4);

        /*System.out.println("Remote put " + value + " at " + siteID + " with Vectorclock: <" + s4.getSessionNumber() +
                ", "+ s4.getSiteId() + ", " + s4.getVectorClockSum() + ", " + s4.getSeq() +">");
        System.out.println(this);*/

        return true;
    }

    // TODO: double-check implementation of this method
    private boolean remoteRemove(K key, S4Vector s4) {
        Slot<K,V> slot = hashTable.get(key);

        while(slot != null && !key.equals(slot.getKey())) {
            slot = slot.getNextSlot();
        }

        if(slot == null) {
            // TODO: Exception text
            throw new NoSlotException("blub");
        }

        if(s4.takesPrecedenceOver(slot.getS4Vector()))  return false;

        if(!slot.isTombstone()) {
            cemetery.enrol(slot);
            slot.makeTombstone();
        }

        slot.setS4Vector(s4);
        slot.setValue(null);

        return true;
    }

    // TODO: method should disable this in production mode (if ther is a large number of elements)
    @Override
    public String toString() {
        // TODO: not show tombstones
        final StringBuilder sb = new StringBuilder("HashTable{\n");
        for(K k : hashTable.keySet()) {
            Slot<K,V> s = hashTable.get(k);
            while(s != null) {
                S4Vector s4 = hashTable.get(k).getS4Vector();
                sb.append("  Key: " + k + ", Value: " + s.getValue() + "\n");
                        /*+ ", S4Vector: <" + s4.getSessionNumber() + ", "+ s4.getSiteId() + ", " + s4.getVectorClockSum()
                                + ", " + s4.getSeq() +">" );*/
                s = s.getNextSlot();
            }
        }

        sb.append('}');
        return sb.toString();
    }
}
