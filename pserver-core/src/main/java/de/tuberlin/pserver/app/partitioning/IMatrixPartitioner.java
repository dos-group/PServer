package de.tuberlin.pserver.app.partitioning;

import de.tuberlin.pserver.app.types.MatrixEntry;
import de.tuberlin.pserver.math.matrix.Matrix;

public interface IMatrixPartitioner {

    // TODO: find a unified way to pass information that are needed for partitioning

    public int getPartitionOfEntry(MatrixEntry entry);

    public Matrix.PartitionShape getPartitionShape();

}
