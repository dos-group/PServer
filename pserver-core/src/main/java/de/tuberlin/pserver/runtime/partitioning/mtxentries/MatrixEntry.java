package de.tuberlin.pserver.runtime.partitioning.mtxentries;

import java.io.Serializable;

/**
 * Represents a matrix entry with row, cols and value.
 */
public interface MatrixEntry extends Serializable {

    public long getRow();

    public long getCol();

    public double getValue();

}
