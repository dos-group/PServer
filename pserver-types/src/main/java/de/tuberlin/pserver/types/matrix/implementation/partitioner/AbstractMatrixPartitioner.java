package de.tuberlin.pserver.types.matrix.implementation.partitioner;

import de.tuberlin.pserver.types.matrix.metadata.DistributedMatrixType;
import de.tuberlin.pserver.types.metadata.DistScheme;

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

    public static AbstractMatrixPartitioner createPartitioner(DistScheme distScheme, DistributedMatrixType distributedMatrixType) {
        switch (distScheme) {
            case H_PARTITIONED:
                return new MatrixRowPartitioner(distributedMatrixType);
            case V_PARTITIONED:
                return new MatrixColumnPartitioner(distributedMatrixType);
            case B_PARTITIONED:
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
