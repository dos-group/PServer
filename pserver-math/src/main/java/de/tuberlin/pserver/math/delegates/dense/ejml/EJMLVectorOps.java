package de.tuberlin.pserver.math.delegates.dense.ejml;


import de.tuberlin.pserver.math.DVector;
import de.tuberlin.pserver.math.delegates.LibraryVectorOps;
import org.ejml.alg.dense.mult.VectorVectorMult;
import org.ejml.data.DenseMatrix64F;
import org.ejml.ops.CommonOps;

public class EJMLVectorOps implements LibraryVectorOps<DVector> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public DVector scale(final DVector X, final double alpha) {
        final DenseMatrix64F x = convertDVectorToDenseVector64F(X);
        CommonOps.scale(alpha, x);
        return X;
    }

    @Override
    public DVector add(final DVector X, final DVector Y) {
        final DenseMatrix64F x = convertDVectorToDenseVector64F(X);
        final DenseMatrix64F y = convertDVectorToDenseVector64F(Y);
        CommonOps.add(x, y, x);
        return X;
    }

    @Override
    public DVector add(final DVector X, final double beta, final DVector Y) {
        final DenseMatrix64F x = convertDVectorToDenseVector64F(X);
        final DenseMatrix64F y = convertDVectorToDenseVector64F(Y);
        CommonOps.add(x, beta, y, x);
        return X;
    }

    @Override
    public double dot(final DVector X, final DVector Y) {
        final DenseMatrix64F x = convertDVectorToDenseVector64F(X);
        final DenseMatrix64F y = convertDVectorToDenseVector64F(Y);
        return VectorVectorMult.innerProd(x, y);
    }

    // ---------------------------------------------------
    // Static Methods.
    // ---------------------------------------------------

    public static DenseMatrix64F convertDVectorToDenseVector64F(final DVector vector) {
        switch (vector.getVectorType()) {
            case ROW_VECTOR:
                return new DenseMatrix64F(vector.size(), 1, true, vector.toArray());
            case COLUMN_VECTOR:
                return new DenseMatrix64F(1, vector.size(), true, vector.toArray());
        }
        throw new IllegalStateException();
    }
}
