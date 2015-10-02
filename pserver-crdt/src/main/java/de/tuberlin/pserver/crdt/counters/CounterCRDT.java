package de.tuberlin.pserver.crdt.counters;

import de.tuberlin.pserver.runtime.DataManager;

public interface CounterCRDT {

    long getCount();
    boolean add(int l, DataManager dataManager);
}
