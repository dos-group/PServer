package de.tuberlin.pserver.radt;

public class Slot<K,V> extends CObject<V> {
    private Slot next;
    private final K key;

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
}
