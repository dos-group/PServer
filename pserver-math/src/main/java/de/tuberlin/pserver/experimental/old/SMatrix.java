package de.tuberlin.pserver.experimental.old;

import java.io.Serializable;

public interface SMatrix extends IMatrixOps<SMatrix, SVector>, Serializable {

    public abstract long numRows();

    public abstract long numCols();

    public abstract Object getInternalMatrix();
}
