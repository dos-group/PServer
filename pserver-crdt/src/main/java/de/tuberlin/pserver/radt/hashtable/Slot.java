package de.tuberlin.pserver.radt.hashtable;

import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.radt.CObject;

public class Slot<K,V> extends CObject<V> {
    private Slot next;
    private final K key;

    // no-arg constructor for serialization
    public Slot() {
        this.next = null;
        this.key = null;
    }

    public Slot(K key, V value, S4Vector s4, Slot next, int[] vectorClock) {
        super(vectorClock, s4, value);
        this.next = next;
        this.key = key;
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
        return getValue() == null;
    }

    public void makeTombstone() {
        setValue(null);
    }

    public boolean hasNextSlot() {
        return this.next != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Slot<?, ?> slot = (Slot<?, ?>) o;

        System.out.println("Slot key: " + this.getKey());
        System.out.println("Slot val: " + this.getValue());
        System.out.println("Slot next: " + this.getNextSlot());

        System.out.println("Curr key: " + slot.getKey());
        System.out.println("Curr val: " + slot.getValue());
        System.out.println("Curr next: " + slot.getNextSlot());

        return this.key == slot.getKey() &&
                this.getValue() == slot.getValue() &&
                this.next == slot.getNextSlot();

    }

    @Override
    public int hashCode() {
        int result = next != null ? next.hashCode() : 0;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        return result;
    }

    // TODO: implement hashcode?
}
