package de.tuberlin.pserver.crdt.registers;

import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

public abstract class AbstractRegister<T> extends AbstractCRDT<T> implements Register<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private T value;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractRegister(String id, int noOfReplicas, ProgramContext programContext) {

        super(id, noOfReplicas, programContext);

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public synchronized T get() {

        return this.value;

    }

    @Override
    public synchronized boolean set(T value) {

        this.value = value;

        return true;

    }
}
