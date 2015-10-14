package de.tuberlin.pserver.crdt.sets;

import de.tuberlin.pserver.runtime.DataManager;

import java.util.Set;

// Todo: extend collections Set?
// Todo: implement iterable?
public interface ISet<T> {

    boolean add(T value);
    boolean remove(T value);
    java.util.Set<T> getSet();

}
