package de.tuberlin.pserver.crdt.sets;

// Todo: extend collections Set?
// Todo: implement iterable?
public interface Set<T> {

    boolean add(T value);
    boolean remove(T value);
    java.util.Set<T> getSet();

}
