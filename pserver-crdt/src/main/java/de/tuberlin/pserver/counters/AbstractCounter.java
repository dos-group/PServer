package de.tuberlin.pserver.counters;

import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.runtime.DataManager;

public abstract class AbstractCounter extends AbstractCRDT implements CounterCRDT {

    public AbstractCounter(String id, DataManager dataManager) {
        super(id, dataManager);
    }
}
