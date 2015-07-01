package de.tuberlin.pserver.app.partitioning;

import de.tuberlin.pserver.app.types.MatrixEntry;
import de.tuberlin.pserver.math.Matrix;

public interface IMatrixPartitioner {

    public int getPartitionOfEntry(MatrixEntry entry);

    public Matrix.Dimension getPartitionedDimension(long rows, long cols);

}
