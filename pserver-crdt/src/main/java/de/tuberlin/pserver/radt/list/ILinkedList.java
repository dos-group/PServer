package de.tuberlin.pserver.radt.list;

import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.radt.RADT;
import de.tuberlin.pserver.radt.S4Vector;

import java.util.List;

public interface ILinkedList<T> extends RADT {
    boolean insert(int index, T value);
    boolean update(int index, T value);
    boolean delete(int index);
    T read(int i);
    List<T> getList();

}
