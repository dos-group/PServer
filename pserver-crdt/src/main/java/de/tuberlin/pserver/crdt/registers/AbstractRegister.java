package de.tuberlin.pserver.crdt.registers;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

public abstract class AbstractRegister<T> extends AbstractCRDT<T> implements Register<T> {
    private T value;

    public AbstractRegister(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
    }

    public synchronized T get() {
        return this.value;
    }

    @Override
    public synchronized boolean set(T value) {
        this.value = value;
        return true;
    }
}
