package de.tuberlin.pserver.crdt.registers;

import de.tuberlin.pserver.runtime.DataManager;

public interface RegisterCRDT<T> {

    boolean set(T element, DataManager dataManager);
    //T getRegister();
}
