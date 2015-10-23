package de.tuberlin.pserver.crdt.radt.hashtable;

import de.tuberlin.pserver.crdt.radt.AbstractRADT;
import de.tuberlin.pserver.crdt.radt.S4Vector;
import de.tuberlin.pserver.runtime.RuntimeManager;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

public abstract class AbstractHashTable<K,V> extends AbstractRADT<V> implements IHashTable<V> {
    protected final Hashtable<K, Slot<K,V>> hashTable;
    protected final Cemetery<V> cemetery;

    protected AbstractHashTable(int size, String id, int noOfReplicas, RuntimeManager runtimeManager) {
        super(size, id, noOfReplicas, runtimeManager);

        // Initialize HashTable
        this.hashTable = new Hashtable<>();

        // Initialize Cemetery
        this.cemetery = new Cemetery<>();
    }
}
