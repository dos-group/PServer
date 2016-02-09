package de.tuberlin.pserver.radt.hashtable;

import de.tuberlin.pserver.radt.RADT;

import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

public interface IHashTable<K,V> extends RADT {

    void put(K key, V value);

    V read(K key);

    boolean remove(K key);

    Hashtable<K,V> getHashtable();

    Set<Map.Entry<K,V>> getEntrySet();


}
