package de.tuberlin.pserver.crdt.operations;

import java.io.Serializable;

public interface IOperation<T> extends Serializable {

    int getType();
    T getValue();
}