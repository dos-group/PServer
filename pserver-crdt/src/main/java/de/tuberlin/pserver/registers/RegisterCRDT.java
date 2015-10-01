package de.tuberlin.pserver.registers;

import de.tuberlin.pserver.runtime.DataManager;

import java.util.Date;

public interface RegisterCRDT<T> {

    boolean set(T element, DataManager dataManager);
    //T getRegister();
}
