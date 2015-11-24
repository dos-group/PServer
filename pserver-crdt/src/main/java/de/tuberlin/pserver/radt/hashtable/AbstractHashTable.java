package de.tuberlin.pserver.radt.hashtable;

import de.tuberlin.pserver.radt.AbstractRADT;
import de.tuberlin.pserver.radt.Cemetery;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.Hashtable;
import java.util.LinkedList;

public abstract class AbstractHashTable<K,V> extends AbstractRADT<V> implements IHashTable<V> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final Hashtable<K, Slot<K,V>> hashTable;
    protected final Cemetery<Slot<K,V>> cemetery;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    protected AbstractHashTable(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);

        // Initialize HashTable
        this.hashTable = new Hashtable<>();

        // Initialize Cemetery
        this.cemetery = new Cemetery<>();
    }
}
