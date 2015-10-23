package de.tuberlin.pserver.crdt.radt.hashtable;

import de.tuberlin.pserver.crdt.radt.RADTOperation;
import de.tuberlin.pserver.crdt.radt.S4Vector;

public class HashTableOperation<K,V> extends RADTOperation<V> {
    private final K key;

    public HashTableOperation(int type, K key, V value, int index, int[] vectorClock, S4Vector s4) {
        super(type, value, index, vectorClock, s4);
        this.key = key;
    }

    public K getKey() {
        return key;
    }

}
