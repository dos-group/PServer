package de.tuberlin.pserver.math.matrix;


import de.tuberlin.pserver.math.matrix.dense.DenseMatrix32F;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix64F;
import de.tuberlin.pserver.math.matrix.sparse.SparseMatrix32F;
import de.tuberlin.pserver.math.matrix.sparse.SparseMatrix64F;

public enum ElementType {

    FLOAT_MATRIX,

    DOUBLE_MATRIX;

    public static ElementType getElementTypeFromClass(final Class<?> clazz) {

        if (clazz == Matrix32F.class ||
            clazz == SparseMatrix32F.class ||
            clazz == DenseMatrix32F.class)

            return FLOAT_MATRIX;

        else if (clazz == Matrix64F.class ||
                 clazz == SparseMatrix64F.class ||
                 clazz == DenseMatrix64F.class)

            return DOUBLE_MATRIX;

        else
            throw new IllegalStateException();
    }
}
