package de.tuberlin.pserver.crdt.sets;

import de.tuberlin.pserver.runtime.DataManager;

import java.util.Set;

public interface ISet<T> {

    boolean add(T value);
    boolean remove(T value);
    java.util.Set<T> getSet();

}
