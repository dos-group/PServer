package de.tuberlin.pserver.radt.hashtable;

import de.tuberlin.pserver.radt.AbstractRADT;
import de.tuberlin.pserver.radt.CObject;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractHashTable<K,V> extends AbstractRADT<V> implements IHashTable<K,V> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final Map<K, Slot<K,V>> map;
    protected final HashTableCemetery<K,V> cemetery;


    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    protected AbstractHashTable(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);

        // Initialize HashTable
        // TODO: setting initial size and load factor can imiprove performance
        this.map = Collections.synchronizedMap(new HashMap<>());

        // Initialize Cemetery
        this.cemetery = new HashTableCemetery<>(map, noOfReplicas, nodeId);
    }

    @Override
    public synchronized Hashtable<K,V> getHashtable() {
        Hashtable<K,V> result = new Hashtable<>();

        map.values().stream()
                .filter(entry -> entry.getValue() != null)
                .forEach(entry -> result.put(entry.getKey(), entry.getValue()));

        return result;
    }

    @Override
    public synchronized Set<Map.Entry<K,V>> getEntrySet() {
        Map<K,V> result = map.values().stream()
                .filter(slot -> slot.getValue() != null)
                .collect(Collectors.<Slot<K,V>, K, V>toMap(Slot::getKey, CObject::getValue));

        return result.entrySet();
    }
}
