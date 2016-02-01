package de.tuberlin.pserver.radt.hashtable;

import de.tuberlin.pserver.radt.RADT;

public interface IHashTable<K,V> extends RADT {

    void put(K key, V value);

    V read(K key);

    boolean remove(K key);


}
