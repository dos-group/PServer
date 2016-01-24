package de.tuberlin.pserver.runtime.state.matrix.entries;

import java.io.Serializable;


public interface Entry<V extends Number> extends Serializable {

    long getRow();

    long getCol();

    V getValue();
}
