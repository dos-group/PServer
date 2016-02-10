package de.tuberlin.pserver.types.matrix;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.types.matrix.f32.Matrix32F;
import de.tuberlin.pserver.types.matrix.f32.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.matrix.f32.sparse.CSRMatrix32F;
import de.tuberlin.pserver.types.matrix.f32.sparse.SparseMatrix32F;
import de.tuberlin.pserver.types.matrix.partitioner.PartitionType;
import de.tuberlin.pserver.types.matrix.properties.ElementType;
import de.tuberlin.pserver.types.matrix.properties.MatrixFormat;

public final class MatrixBuilder {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private long rows, cols;

    private MatrixFormat matrixFormat;

    private ElementType elementType;

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

    public MatrixBuilder matrixFormat(final MatrixFormat matrixFormat) {
        this.matrixFormat = Preconditions.checkNotNull(matrixFormat);
        return this;
    }

    public MatrixBuilder elementType(final ElementType type) {
        this.elementType = Preconditions.checkNotNull(type);
        return this;
    }

    public Matrix32F build() {
        return build(-1, null, PartitionType.NO_PARTITIONER, null);
    }

    public Matrix32F build(int nodeID, int[] nodes, PartitionType partitionType) {
        return build(nodeID, nodes, partitionType, null);
    }

    public Matrix32F build(int nodeID, int[] nodes, PartitionType partitionType, final float[] data) {
        switch (matrixFormat) {
            case SPARSE_FORMAT:
                switch (elementType) {
                    case FLOAT_MATRIX:
                        return new SparseMatrix32F(nodeID, nodes, partitionType, rows, cols, data);
                    case DOUBLE_MATRIX:
                        throw new IllegalStateException();
                } break;
            case DENSE_FORMAT:
                switch (elementType) {
                    case FLOAT_MATRIX:
                        return new DenseMatrix32F(nodeID, nodes, partitionType, rows, cols, data);
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
        rows         = -1;
        cols         = -1;
        matrixFormat = MatrixFormat.DENSE_FORMAT;
        elementType  = ElementType.FLOAT_MATRIX;
    }
}
