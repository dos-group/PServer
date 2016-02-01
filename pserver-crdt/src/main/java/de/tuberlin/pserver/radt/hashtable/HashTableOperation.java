package de.tuberlin.pserver.radt.hashtable;

import de.tuberlin.pserver.radt.RADTOperation;
import de.tuberlin.pserver.radt.S4Vector;

public class HashTableOperation<K,V> extends RADTOperation<V> {
    private final K key;

    public HashTableOperation(OpType type, K key, V value, int[] vectorClock, S4Vector s4) {
        super(type, value, vectorClock, s4);
        this.key = key;
    }

    public K getKey() {
        return key;
    }

}
