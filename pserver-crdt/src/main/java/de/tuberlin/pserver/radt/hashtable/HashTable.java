package de.tuberlin.pserver.radt.hashtable;

import de.tuberlin.pserver.crdt.exceptions.IllegalOperationException;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.radt.Cemetery;
import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

public class HashTable<K,V> extends AbstractHashTable<K,V> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Cemetery<Slot<K,V>> cemetery;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public HashTable(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
        cemetery = new Cemetery<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void put(K key, V value) {
        int[] clock = increaseVectorClock();
        S4Vector s4 = new S4Vector(sessionID, nodeId, clock, 0);

        Slot<K,V> slot = localPut(key, value, s4, clock);


        broadcast(new HashTableOperation<>(Operation.OpType.PUT, key, slot, clock, s4));
    }

    @Override
    public V read(K key) {
        if(key == null) return null;

        Slot<K,V> slot = hashTable.get(key);

        if(slot == null || slot.isTombstone()) return null;
        else return slot.getValue();
    }

    @Override
    public boolean remove(K key) {
        Slot<K,V> slot = localRemove(key);

        if(slot != null) {
            int[] clock = increaseVectorClock();
            S4Vector s4 = new S4Vector(sessionID, nodeId, clock, 0);

            broadcast(new HashTableOperation<>(Operation.OpType.REMOVE, key, slot, clock, s4));
            return true;
        }
        else {
            return false;
        }
    }

    // TODO: method should disable this in production mode (if there is a large number of elements)
    @Override
    public String toString() {
        // TODO: show or not show tombstones
        final StringBuilder sb = new StringBuilder("HashTable{\n");
        for(K k : hashTable.keySet()) {
            Slot<K,V> s = hashTable.get(k);
            while(s != null) {
                sb.append("  Key: ").append(k).append(", Value: ").append(s.getValue()).append("\n");
                s = s.getNextSlot();
            }
        }

        sb.append('}');
        return sb.toString();
    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    @Override
    protected boolean update(int srcNodeId, Operation<?> op) {
        @SuppressWarnings("unchecked")
        HashTableOperation<K,Slot<K,V>> radtOp = (HashTableOperation<K,Slot<K,V>>) op;

        if(radtOp.getType() == Operation.OpType.PUT) {
            return remotePut(radtOp.getKey(), radtOp.getValue().getValue(), radtOp.getS4Vector());
        }
        else if(radtOp.getType() == Operation.OpType.REMOVE) {
            return remoteRemove(radtOp.getKey(), radtOp.getS4Vector());
        }
        else {
            throw new IllegalArgumentException("HashTable RADTs do not allow the " + op.getType() + " operation.");
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------


    private Slot<K,V> localPut(K key, V value, S4Vector s4, int[] clock) {
        // Seperate chaining scheme is used to handle collisionsbout collLinkedList<isions?
        // TODO: is collision handling working here with just setting slot.getNextSlot()?
        Slot<K,V> slot = hashTable.get(key);

        if(slot != null) {
            slot = new Slot<>(key, value, s4, slot.getNextSlot(), clock);
            hashTable.put(key, slot);
            return slot;
        } else {
            slot = new Slot<>(key, value, s4, null, clock);
            hashTable.put(key, slot);
            return slot;
        }
    }

    private Slot<K,V> localRemove(K key) {
        // TODO: what about chaining scheme?
        Slot<K,V> slot = hashTable.get(key);

        if(slot == null){
            return null;
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
        else if (slot != null && slot.isTombstone()) {
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
}