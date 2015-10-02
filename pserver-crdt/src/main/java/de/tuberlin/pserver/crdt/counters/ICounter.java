package de.tuberlin.pserver.crdt.counters;

import de.tuberlin.pserver.runtime.DataManager;

public interface ICounter {

    long getCount();
    boolean add(int l);
}
