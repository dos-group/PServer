package de.tuberlin.pserver.crdt.sets;

public interface Set<T> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    boolean add(T value);

    boolean remove(T value);

    java.util.Set<T> getSet();

}
