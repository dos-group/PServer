package de.tuberlin.pserver.playground.old;

import java.io.Serializable;

public interface SVector extends IVectorOps<SVector>, Serializable {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public enum VectorType {

        ROW_VECTOR,

        COLUMN_VECTOR
    }

    // ---------------------------------------------------
    // Abstract Methods.
    // ---------------------------------------------------

    public abstract long size();

    public abstract Object getInternalVector();

    public abstract VectorType getVectorType();
}
