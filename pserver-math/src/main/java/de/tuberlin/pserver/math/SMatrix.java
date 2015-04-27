package de.tuberlin.pserver.math;

import java.io.Serializable;

public interface SMatrix extends IMatrixOps<SMatrix, SVector>, Serializable {

    public abstract long numRows();

    public abstract long numCols();

    public abstract Object getInternalMatrix();
}
