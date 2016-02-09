package de.tuberlin.pserver.radt.hashtable;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.crdt.exceptions.CRDTException;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

public class HashTable<K,V> extends AbstractHashTable<K,V> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------


    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public HashTable(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
        ready();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public synchronized void put(K key, V value) {
        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");
        Preconditions.checkArgument(key != null);
        Preconditions.checkArgument(value != null);

        int[] clock = increaseVectorClock();
        S4Vector s4 = new S4Vector(nodeId, clock);

        map.put(key, new Slot<>(key, value, s4));

        broadcast(new HashTableOperation<>(Operation.OpType.PUT, key, value, clock, s4));
    }

    @Override
    public synchronized V read(K key) {
        if(key == null) return null;

        Slot<K,V> slot = map.get(key);

        if(slot == null || slot.isTombstone()) return null;

        return slot.getValue();
    }

    @Override
    public synchronized boolean remove(K key) {
        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

        Slot<K,V> slot = map.get(key);

        if(slot == null) return false;
        else cemetery.enrol(nodeId, slot);

        int[] clock = increaseVectorClock();
        S4Vector s4 = new S4Vector(nodeId, clock);

        broadcast(new HashTableOperation<>(Operation.OpType.REMOVE, key, null, clock, s4));
        return true;
    }

    // TODO: should disable this in production mode (if there is a large number of elements)?
    @Override
    public synchronized String toString() {
        final StringBuilder sb = new StringBuilder("HashTable{\n");
        for(Slot<K,V> slot : map.values()) {
            sb.append("  Key: ")
                    .append(slot.getKey())
                    .append(", Value: ")
                    .append(slot.getValue() != null ? slot.getValue() : "tombstone")
                    .append("\n");
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
        HashTableOperation<K,V> hashOp = (HashTableOperation<K,V>) op;
        boolean result;

        switch(hashOp.getType()) {
            case PUT:
                result = remotePut(hashOp.getKey(), hashOp.getValue(), hashOp.getS4Vector());
                cemetery.updateAndPurge(hashOp.getS4Vector().getSiteId(), hashOp.getVectorClock());
                return result;
            case REMOVE:
                result = remoteRemove(hashOp.getKey(), hashOp.getS4Vector());
                cemetery.updateAndPurge(hashOp.getS4Vector().getSiteId(), hashOp.getVectorClock());
                return result;
            default:
                throw new IllegalArgumentException("HashTable RADTs do not allow the " + op.getType() + " operation.");
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private synchronized boolean remotePut(K key, V value, S4Vector s4) {
        Slot<K,V> currSlot = map.get(key);
        //System.out.println("[DEBUG]" + nodeId+"|"+key + " Remote Put, Value: " + value);

        if(currSlot == null) {
            //System.out.println("[DEBUG]" + nodeId+"|"+key + " Remote Put, slot is empty");
            map.put(key, new Slot<>(key, value, s4));
            return true;
        }

        if(s4.precedes(currSlot.getS4Vector())) return false;

        if(currSlot.isTombstone()) cemetery.withdraw(currSlot);

        currSlot.setValue(value);
        currSlot.setS4Vector(s4);

        return true;
    }

    // TODO: double-check implementation of this method
    private synchronized boolean remoteRemove(K key, S4Vector s4) {
        Slot<K,V> currSlot = map.get(key);

        //System.out.println("[DEBUG]" + nodeId + "|" + key + " Remote Remove");

        if(currSlot == null) {
            throw new CRDTException("Attempting to remote remove a slot which doesn't exist at the replica " + nodeId);
        }

        if(s4.precedes(currSlot.getS4Vector()))  return false;

        if(!currSlot.isTombstone()) cemetery.enrol(nodeId, currSlot);

        currSlot.setS4Vector(s4);

        return true;
    }
}