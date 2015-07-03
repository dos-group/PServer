package de.tuberlin.pserver.app.types;

import java.io.Serializable;

/**
 * Represents a matrix entry with row, cols and value.
 */
public interface MatrixEntry extends Serializable {

    public long getRow();

    public long getCol();

    public double getValue();

}
