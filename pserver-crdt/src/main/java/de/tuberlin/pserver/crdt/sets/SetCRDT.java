package de.tuberlin.pserver.crdt.sets;

import de.tuberlin.pserver.runtime.DataManager;

import java.util.Set;

public interface SetCRDT<T> {

    boolean add(T value, DataManager dataManager);
    boolean remove(T value, DataManager dataManager);
    Set<T> getSet();

}
