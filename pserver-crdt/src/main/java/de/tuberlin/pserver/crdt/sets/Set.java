package de.tuberlin.pserver.crdt.sets;

// Todo: implement iterable?
public interface Set<T> {

    boolean add(T value);
    boolean remove(T value);
    java.util.Set<T> getSet();

    // Todo: possibly have an applyToElements method

}
