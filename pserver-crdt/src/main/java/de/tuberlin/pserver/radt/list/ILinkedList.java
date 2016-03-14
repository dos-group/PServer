package de.tuberlin.pserver.radt.list;

import de.tuberlin.pserver.radt.RADT;

import java.util.List;

public interface ILinkedList<T> extends RADT {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    boolean insert(int index, T value);

    boolean update(int index, T value);

    boolean delete(int index);

    T read(int i);

    List<T> getList();
}
