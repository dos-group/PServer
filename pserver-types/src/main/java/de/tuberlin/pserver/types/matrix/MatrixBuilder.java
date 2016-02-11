package de.tuberlin.pserver.types.matrix;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.types.DistributedTypeBuilder;
import de.tuberlin.pserver.types.matrix.annotation.MatrixDeclaration;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.sparse.CSRMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.sparse.SparseMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.properties.ElementType;
import de.tuberlin.pserver.types.matrix.implementation.properties.MatrixType;
import de.tuberlin.pserver.types.metadata.DistScheme;
import org.apache.commons.lang3.tuple.Triple;

import java.util.HashMap;
import java.util.Map;

public final class MatrixBuilder extends DistributedTypeBuilder<Matrix32F, MatrixDeclaration> {

    // ---------------------------------------------------
    // Type Registration.
    // ---------------------------------------------------

    private static final Map<Class<?>, Triple<Class<?>, MatrixType, ElementType>> registeredMatrixTypes;

    static {
        registeredMatrixTypes = new HashMap<>();
        registeredMatrixTypes.put(Matrix32F.class,          Triple.of(DenseMatrix32F.class,     MatrixType.DENSE_FORMAT,    ElementType.FLOAT_MATRIX));
        registeredMatrixTypes.put(DenseMatrix32F.class,     Triple.of(DenseMatrix32F.class,     MatrixType.DENSE_FORMAT,    ElementType.FLOAT_MATRIX));
        registeredMatrixTypes.put(SparseMatrix32F.class,    Triple.of(SparseMatrix32F.class,    MatrixType.SPARSE_FORMAT,   ElementType.FLOAT_MATRIX));
        registeredMatrixTypes.put(CSRMatrix32F.class,       Triple.of(CSRMatrix32F.class,       MatrixType.CSR_FORMAT,      ElementType.FLOAT_MATRIX));
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private DistScheme distScheme;

    private long rows, cols;

    private MatrixType matrixType;

    private ElementType elementType;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MatrixBuilder() { reset(); }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public MatrixBuilder distributionScheme(final DistScheme distScheme) {
        this.distScheme = Preconditions.checkNotNull(distScheme);
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

    public MatrixBuilder inferMatrixType(final Class<?> type) {
        Triple<Class<?>, MatrixType, ElementType> inferedType = registeredMatrixTypes.get(type);
        this.matrixType     = inferedType.getMiddle();
        this.elementType    = inferedType.getRight();
        return this;
    }

    // ---------------------------------------------------

    @Override
    public Matrix32F build(int nodeID, MatrixDeclaration declaration) {
        distributionScheme(declaration.distScheme);
        dimension(declaration.rows, declaration.cols);
        inferMatrixType(declaration.type);
        return build(nodeID, declaration.nodes);
    }

    @Override
    public Matrix32F build(int nodeID, int[] nodes) {
        Matrix32F matrix;
        switch (matrixType) {
            case SPARSE_FORMAT:
                switch (elementType) {
                    case FLOAT_MATRIX: matrix = new SparseMatrix32F(nodeID, nodes, distScheme, rows, cols); break;
                    case DOUBLE_MATRIX: throw new IllegalStateException();
                    default: throw new IllegalStateException();
                } break;
            case DENSE_FORMAT:
                switch (elementType) {
                    case FLOAT_MATRIX: matrix = new DenseMatrix32F(nodeID, nodes, distScheme, rows, cols, null); break;
                    case DOUBLE_MATRIX: throw new IllegalStateException();
                    default: throw new IllegalStateException();
                } break;
            case CSR_FORMAT:
                switch (elementType) {
                    case FLOAT_MATRIX: matrix = new CSRMatrix32F(nodeID, nodes, distScheme, rows, cols); break;
                    case DOUBLE_MATRIX: throw new IllegalStateException();
                    default: throw new IllegalStateException();
                } break;
            default: throw new IllegalStateException();
        }
        reset();
        return matrix;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void reset() {
        distScheme = DistScheme.LOCAL;
        this.rows = -1;
        this.cols = -1;
        this.matrixType = MatrixType.DENSE_FORMAT;
        this.elementType = ElementType.FLOAT_MATRIX;
    }
}
