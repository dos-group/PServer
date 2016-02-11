package de.tuberlin.pserver.types.matrix;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.types.DistributedTypeBuilder;
import de.tuberlin.pserver.types.matrix.annotation.MatrixDeclaration;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.f32.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.f32.sparse.CSRMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.f32.sparse.SparseMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.properties.ElementType;
import de.tuberlin.pserver.types.matrix.implementation.properties.MatrixType;
import de.tuberlin.pserver.types.metadata.DistributionScheme;

public final class MatrixBuilder extends DistributedTypeBuilder<Matrix32F, MatrixDeclaration> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private long rows, cols;

    private MatrixType matrixType;

    private ElementType elementType;

    private DistributionScheme distributionScheme;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MatrixBuilder() { clear(); }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public MatrixBuilder distributionScheme(final DistributionScheme distributionScheme) {
        this.distributionScheme = Preconditions.checkNotNull(distributionScheme);
        return this;
    }

    public MatrixBuilder dimension(final long rows, final long cols) {
        this.rows = rows;
        this.cols = cols;
        return this;
    }

    public MatrixBuilder matrixType(final MatrixType matrixType) {
        this.matrixType = Preconditions.checkNotNull(matrixType);
        return this;
    }

    public MatrixBuilder elementType(final ElementType type) {
        this.elementType = Preconditions.checkNotNull(type);
        return this;
    }

    // ---------------------------------------------------

    @Override
    public Matrix32F build(int nodeID, MatrixDeclaration declaration) {
        distributionScheme(declaration.distributionScheme);
        dimension(declaration.rows, declaration.cols);
        matrixType(declaration.type);
        elementType(declaration.elementType);
        return build(nodeID, declaration.nodes);
    }

    @Override
    public Matrix32F build(int nodeID, int[] nodes) {
        Matrix32F matrix;
        switch (matrixType) {
            case SPARSE_FORMAT:
                switch (elementType) {
                    case FLOAT_MATRIX: matrix = new SparseMatrix32F(nodeID, nodes, distributionScheme, rows, cols); break;
                    case DOUBLE_MATRIX: throw new IllegalStateException();
                    default: throw new IllegalStateException();
                } break;
            case DENSE_FORMAT:
                switch (elementType) {
                    case FLOAT_MATRIX: matrix = new DenseMatrix32F(nodeID, nodes, distributionScheme, rows, cols, null); break;
                    case DOUBLE_MATRIX: throw new IllegalStateException();
                    default: throw new IllegalStateException();
                } break;
            case CSR_FORMAT:
                switch (elementType) {
                    case FLOAT_MATRIX: matrix = new CSRMatrix32F(nodeID, nodes, distributionScheme, rows, cols); break;
                    case DOUBLE_MATRIX: throw new IllegalStateException();
                    default: throw new IllegalStateException();
                } break;
            default: throw new IllegalStateException();
        }
        clear();
        return matrix;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void clear() {
        rows = -1;
        cols = -1;
        matrixType = MatrixType.DENSE_FORMAT;
        elementType = ElementType.FLOAT_MATRIX;
        distributionScheme = DistributionScheme.LOCAL;
    }
}
