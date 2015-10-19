package de.tuberlin.pserver.utils;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.*;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix32F;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix64F;
import de.tuberlin.pserver.math.matrix.sparse.SparseMatrix32F;
import de.tuberlin.pserver.math.matrix.sparse.SparseMatrix64F;

public final class MatrixBuilder {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private long rows, cols;

    private Format format;

    private Layout layout;

    private ElementType elementType;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MatrixBuilder() {
        clear();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public MatrixBuilder dimension(final long rows, final long cols) {
        this.rows = rows;
        this.cols = cols;
        return this;
    }

    public MatrixBuilder format(final Format format) {
        this.format = Preconditions.checkNotNull(format);
        return this;
    }

    public MatrixBuilder layout(final Layout layout) {
        this.layout = Preconditions.checkNotNull(layout);
        return this;
    }

    public MatrixBuilder elementType(final ElementType type) {
        this.elementType = Preconditions.checkNotNull(type);
        return this;
    }

    /*public MatrixBuilder elementType(final Class<?> type) {
        if (type == Matrix32F.class || type == DenseMatrix32F.class || type == SparseMatrix32F.class)
            this.elementType = ElementType.FLOAT_MATRIX;
        else if (type == Matrix64F.class || type == DenseMatrix64F.class || type == SparseMatrix64F.class)
            this.elementType = ElementType.DOUBLE_MATRIX;
        else
            throw new IllegalStateException();
        return this;
    }*/

    @SuppressWarnings("unchecked")
    public <MAT extends Matrix> MAT build() {
        MAT m = null;
        switch (format) {
            case SPARSE_FORMAT:
                switch (elementType) {
                    case FLOAT_MATRIX:
                        m = (MAT)new SparseMatrix32F(rows, cols, layout);
                        break;
                    case DOUBLE_MATRIX:
                        m = (MAT)new SparseMatrix64F(rows, cols, layout);
                        break;
                }
            case DENSE_FORMAT:
                switch (elementType) {
                    case FLOAT_MATRIX:
                        m = (MAT)new DenseMatrix32F(rows, cols, layout);
                        break;
                    case DOUBLE_MATRIX:
                        m = (MAT)new DenseMatrix64F(rows, cols, layout);
                        break;
                }
        }
        return m;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void clear() {
        rows        = -1;
        cols        = -1;
        format      = Format.DENSE_FORMAT;
        layout      = Layout.ROW_LAYOUT;
        elementType = ElementType.FLOAT_MATRIX;
    }
}
