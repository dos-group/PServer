package de.tuberlin.pserver.runtime.partitioning.mtxentries;

import java.io.Serializable;

/**
 * Represents a matrix entry with row, cols and value.
 */
public interface MatrixEntry<V extends Number> extends Serializable {

    public long getRow();

    public long getCol();

    public V getValue();

}
