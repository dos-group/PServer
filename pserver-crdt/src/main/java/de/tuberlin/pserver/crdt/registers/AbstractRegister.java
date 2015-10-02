package de.tuberlin.pserver.crdt.registers;

import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.runtime.DataManager;

public abstract class AbstractRegister<T> extends AbstractCRDT<T> implements Register<T> {
    protected T value;

    public AbstractRegister(String id, DataManager dataManager) {
        super(id, dataManager);
    }


    public T getValue() {
        return this.value;
    }

    protected void setValue(T value) {
        this.value = value;
    }
}
