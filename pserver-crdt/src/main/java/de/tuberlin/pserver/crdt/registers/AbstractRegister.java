package de.tuberlin.pserver.crdt.registers;

import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.runtime.RuntimeManager;

public abstract class AbstractRegister<T> extends AbstractCRDT<T> implements Register<T> {
    protected T value;

    public AbstractRegister(String id, int noOfReplicas, RuntimeManager runtimeManager) {
        super(id, noOfReplicas, runtimeManager);
    }


    public T getValue() {
        return this.value;
    }

    protected void setValue(T value) {
        this.value = value;
    }
}
