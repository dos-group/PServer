package de.tuberlin.pserver.crdt;

import java.io.Serializable;

public interface Operation<T> extends Serializable {

    int getType();
    T getValue();
}