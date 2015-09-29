package de.tuberlin.pserver.runtime.partitioning.mtxentries;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.partitioning.IMatrixPartitioner;
import de.tuberlin.pserver.runtime.partitioning.PartitioningConfig;

/**
 * Partitions a matrix into one partition of the same size as the matrix. Used if no partitioning is applied.
 */
public class CompletePartitioner extends IMatrixPartitioner {

    private final Matrix.PartitionShape shape;

    public CompletePartitioner(PartitioningConfig config, SlotContext context) {
        super(config, context);
        shape = new Matrix.PartitionShape(config.matrixNumRows, config.matrixNumCols, 0, 0);
    }

    @Override
    public int getPartitionOfEntry(MatrixEntry entry) {
        return context.runtimeContext.nodeID;
    }

    @Override
    public Matrix.PartitionShape getPartitionShape() {
        return shape;
    }

    @Override
    public long translateGlobalToLocalRow(long row) throws IllegalArgumentException {
        return row;
    }

    @Override
    public long translateGlobalToLocalCol(long col) throws IllegalArgumentException {
        return col;
    }

    @Override
    public long translateLocalToGlobalRow(long row) throws IllegalArgumentException {
        return row;
    }

    @Override
    public long translateLocalToGlobalCol(long col) throws IllegalArgumentException {
        return col;
    }
}
