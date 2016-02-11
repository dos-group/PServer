package de.tuberlin.pserver.types.matrix.implementation.partitioner;

import de.tuberlin.pserver.types.matrix.implementation.f32.entries.Entry32F;
import de.tuberlin.pserver.types.matrix.metadata.DistributedMatrixType;
import de.tuberlin.pserver.types.metadata.DistributionScheme;

public abstract class AbstractMatrixPartitioner {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final DistributedMatrixType distributedMatrixType;

    protected final MatrixPartitionShape matrixShape;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractMatrixPartitioner(DistributedMatrixType distributedMatrixType) {
        this.distributedMatrixType = distributedMatrixType;
        this.matrixShape = computeMatrixPartitionShape();
    }

    // ---------------------------------------------------
    // Factory Method.
    // ---------------------------------------------------

    public static AbstractMatrixPartitioner createPartitioner(DistributionScheme distributionScheme, DistributedMatrixType distributedMatrixType) {
        switch (distributionScheme) {
            case HORIZONTAL_PARTITIONED:
                return new MatrixRowPartitioner(distributedMatrixType);
            case VERTICAL_PARTITIONED:
                return new MatrixColumnPartitioner(distributedMatrixType);
            case BLOCK_PARTITIONED:
                return new MatrixBlockPartitioner(distributedMatrixType);
            default:
                return null;
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public MatrixPartitionShape matrixPartitionShape() { return matrixShape; }

    // ---------------------------------------------------
    // Public Abstract Methods.
    // ---------------------------------------------------

    public abstract int getPartitionOfEntry(long row, long col);

    public abstract int getPartitionOfEntry(Entry32F entry);

    public abstract long globalToLocalRow(long row);

    public abstract long globalToLocalColumn(long col);

    public abstract long localToGlobalRow(long row);

    public abstract long localToGlobalColumn(long col);

    public abstract int getNumRowPartitions();

    public abstract int getNumColPartitions();

    // ---------------------------------------------------
    // Protected Abstract Methods.
    // ---------------------------------------------------

    protected abstract MatrixPartitionShape computeMatrixPartitionShape();
}
