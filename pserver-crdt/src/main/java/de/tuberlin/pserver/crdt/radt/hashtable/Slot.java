package de.tuberlin.pserver.crdt.radt.hashtable;

import de.tuberlin.pserver.crdt.radt.S4Vector;

public class Slot<K,V> {
    private V value;
    private S4Vector s4;
    private Slot next;
    private final K key;

    public Slot(K key, V value, S4Vector s4, Slot next) {
        this.value = value;
        this.s4 = s4;
        this.next = next;
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public S4Vector getS4Vector() {
        return s4;
    }

    public void setS4Vector(S4Vector s4) {
        this.s4 = s4;
    }

    public Slot<K,V> getNextSlot() {
        return next;
    }

    public void setNextSlot(Slot slot) {
        this.next = slot;
    }

    public K getKey() {
        return key;
    }

    public boolean isTombstone() {
        return value == null;
    }
}
