package de.tuberlin.pserver.types.matrix.implementation.partitioner;


import de.tuberlin.pserver.types.matrix.typeinfo.MatrixTypeInfo;

public class MatrixColumnPartitioner extends AbstractMatrixPartitioner {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MatrixColumnPartitioner() {}

    public MatrixColumnPartitioner(MatrixTypeInfo distributedMatrixType) {
        super(distributedMatrixType);

        // TODO: IMPLEMENT!

        throw new UnsupportedOperationException();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override public int getPartitionOfEntry(long row, long col) { return 0; }

    @Override public long globalToLocalRow(long row) { return 0; }

    @Override public long globalToLocalColumn(long col) { return 0; }

    @Override public long localToGlobalRow(long row) { return 0; }

    @Override public long localToGlobalColumn(long col) { return 0; }

    @Override public int getNumRowPartitions() { return 0; }

    @Override public int getNumColPartitions() { return 0; }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    @Override protected MatrixPartitionShape computeMatrixPartitionShape() { return null; }
}
