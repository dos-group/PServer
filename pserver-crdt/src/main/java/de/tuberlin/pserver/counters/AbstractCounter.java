package de.tuberlin.pserver.counters;

import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.runtime.DataManager;


public abstract class AbstractCounter extends AbstractCRDT implements CounterCRDT {
    private int buffer;


    public AbstractCounter(DataManager dataManager) {
        super(dataManager);
    }

    protected void buffer(int value) {
        buffer += value;
    }

    public int getBuffer() {
        return buffer;
    }

    protected void resetBuffer() {
        this.buffer = 0;
    }

    public abstract int getCount();
}
