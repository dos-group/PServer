package de.tuberlin.pserver.counters;

import de.tuberlin.pserver.runtime.DataManager;

import javax.xml.crypto.Data;

public interface CounterCRDT {

    int getCount();
    boolean add(int l, DataManager dataManager);
}
