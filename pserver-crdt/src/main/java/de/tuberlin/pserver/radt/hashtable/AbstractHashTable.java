package de.tuberlin.pserver.radt.hashtable;

import de.tuberlin.pserver.radt.AbstractRADT;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.Hashtable;

public abstract class AbstractHashTable<K,V> extends AbstractRADT<V> implements IHashTable<K,V> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    //TODO: use a hashmap
    protected final Hashtable<K, Slot<K,V>> hashTable;
    protected final HashTableCemetery<K,V> cemetery;


    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    protected AbstractHashTable(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);

        // Initialize HashTable
        this.hashTable = new Hashtable<>();

        // Initialize Cemetery
        this.cemetery = new HashTableCemetery<>(hashTable, noOfReplicas, nodeId);
    }
}
