package de.tuberlin.pserver.types.matrix.implementation.partitioner;

import de.tuberlin.pserver.types.matrix.typeinfo.MatrixTypeInfo;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

public abstract class AbstractMatrixPartitioner {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final MatrixTypeInfo distributedMatrixType;

    protected final MatrixPartitionShape matrixShape;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractMatrixPartitioner() {
        this.distributedMatrixType = null;
        this.matrixShape = null;
    }

    public AbstractMatrixPartitioner(MatrixTypeInfo distributedMatrixType) {
        this.distributedMatrixType = distributedMatrixType;
        this.matrixShape = computeMatrixPartitionShape();
    }

    // ---------------------------------------------------
    // Factory Method.
    // ---------------------------------------------------

    public static AbstractMatrixPartitioner createPartitioner(DistScheme distScheme, MatrixTypeInfo distributedMatrixType) {
        switch (distScheme) {
            case H_PARTITIONED:
                return new MatrixRowPartitioner(distributedMatrixType);
            case V_PARTITIONED:
                return new MatrixColumnPartitioner(distributedMatrixType);
            case B_PARTITIONED:
                return new MatrixBlockPartitioner(distributedMatrixType);
            default:
                return new NoPartitioner(distributedMatrixType);
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
