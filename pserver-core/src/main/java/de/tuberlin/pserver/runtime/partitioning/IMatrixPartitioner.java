package de.tuberlin.pserver.runtime.partitioning;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MatrixEntry;

public interface IMatrixPartitioner {

    // TODO: find a unified way to pass information that are needed for partitioning

    public int getPartitionOfEntry(MatrixEntry entry);

    public Matrix.PartitionShape getPartitionShape();

}
