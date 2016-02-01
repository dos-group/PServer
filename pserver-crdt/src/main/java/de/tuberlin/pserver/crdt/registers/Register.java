package de.tuberlin.pserver.crdt.registers;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.runtime.RuntimeManager;

public interface Register<T> extends CRDT<T> {

    boolean set(T element);
    // This may not make sense as Multivalue register needs Set<T> as a return type :/
    // T getRegister();
}
