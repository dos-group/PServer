package de.tuberlin.pserver.crdt.registers;

import de.tuberlin.pserver.runtime.RuntimeManager;

public interface Register<T> {

    boolean set(T element);
    //T getRegister();
}
