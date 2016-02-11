package de.tuberlin.pserver.types.matrix;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.types.DistributedTypeBuilder;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.f32.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.f32.sparse.CSRMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.f32.sparse.SparseMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.partitioner.PartitionType;
import de.tuberlin.pserver.types.matrix.implementation.properties.ElementType;
import de.tuberlin.pserver.types.matrix.implementation.properties.MatrixType;

public final class MatrixBuilder extends DistributedTypeBuilder<Matrix32F> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private long rows, cols;

    private MatrixType matrixType;

    private ElementType elementType;

    private PartitionType partitionType;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MatrixBuilder() { clear(); }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public MatrixBuilder dimension(final long rows, final long cols) {
        this.rows = rows;
        this.cols = cols;
        return this;
    }

    public MatrixBuilder matrixFormat(final MatrixType matrixType) {
        this.matrixType = Preconditions.checkNotNull(matrixType);
        return this;
    }

    public MatrixBuilder elementType(final ElementType type) {
        this.elementType = Preconditions.checkNotNull(type);
        return this;
    }

    public MatrixBuilder partitionType(final PartitionType partitionType) {
        this.partitionType = Preconditions.checkNotNull(partitionType);
        return this;
    }

    // ---------------------------------------------------

    @Override
    public Matrix32F build(int nodeID, int[] nodes) {
        switch (matrixType) {
            case SPARSE_FORMAT:
                switch (elementType) {
                    case FLOAT_MATRIX:
                        return new SparseMatrix32F(nodeID, nodes, partitionType, rows, cols, null);
                    case DOUBLE_MATRIX:
                        throw new IllegalStateException();
                } break;
            case DENSE_FORMAT:
                switch (elementType) {
                    case FLOAT_MATRIX:
                        return new DenseMatrix32F(nodeID, nodes, partitionType, rows, cols, null);
                    case DOUBLE_MATRIX:
                        throw new IllegalStateException();
                } break;
            case CSR_FORMAT:
                switch (elementType) {
                    case FLOAT_MATRIX:
                        return new CSRMatrix32F(nodeID, nodes, partitionType, rows, cols);
                    case DOUBLE_MATRIX:
                        throw new IllegalStateException();
                } break;
        }
        throw new IllegalStateException();
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void clear() {
        rows          = -1;
        cols          = -1;
        matrixType    = MatrixType.DENSE_FORMAT;
        elementType   = ElementType.FLOAT_MATRIX;
        partitionType = PartitionType.NO_PARTITIONER;
    }
}
