package de.tuberlin.pserver.math.delegates.dense.ejml;


import de.tuberlin.pserver.math.delegates.LibraryVectorOps;
import de.tuberlin.pserver.math.Vector;
import org.ejml.alg.dense.mult.VectorVectorMult;
import org.ejml.data.DenseMatrix64F;
import org.ejml.ops.CommonOps;
import org.ejml.ops.NormOps;

public class EJMLVectorOps implements LibraryVectorOps<Vector> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public Vector mul(final Vector X, final double alpha) {
        final DenseMatrix64F x = convertDVectorToDenseVector64F(X);
        CommonOps.scale(alpha, x);
        return X;
    }

    @Override
    public Vector div(final Vector X, final double alpha) {
        final DenseMatrix64F x = convertDVectorToDenseVector64F(X);
        CommonOps.scale(1 / alpha, x);
        return X;
    }

    @Override
    public Vector add(final Vector X, final Vector Y) {
        final DenseMatrix64F x = convertDVectorToDenseVector64F(X);
        final DenseMatrix64F y = convertDVectorToDenseVector64F(Y);
        CommonOps.add(x, y, x);
        return X;
    }

    @Override
    public Vector sub(final Vector X, final Vector Y) {
        final DenseMatrix64F x = convertDVectorToDenseVector64F(X);
        final DenseMatrix64F y = convertDVectorToDenseVector64F(Y);
        CommonOps.sub(x, y, x);
        return X;
    }

    @Override
    public Vector add(final Vector X, final double beta, final Vector Y) {
        final DenseMatrix64F x = convertDVectorToDenseVector64F(X);
        final DenseMatrix64F y = convertDVectorToDenseVector64F(Y);
        CommonOps.add(x, beta, y, x);
        return X;
    }

    @Override
    public double dot(final Vector X, final Vector Y) {
        final DenseMatrix64F x = convertDVectorToDenseVector64F(X);
        final DenseMatrix64F y = convertDVectorToDenseVector64F(Y);
        return VectorVectorMult.innerProd(x, y);
    }

    @Override
    public double norm(final Vector X, final double power) {
        final DenseMatrix64F x = convertDVectorToDenseVector64F(X);
        return NormOps.fastNormP(x, power);
    }

    @Override
    public double maxValue(final Vector X) {
        final DenseMatrix64F x = convertDVectorToDenseVector64F(X);
        return CommonOps.elementMax(x);
    }

    @Override
    public double minValue(final Vector X) {
        final DenseMatrix64F x = convertDVectorToDenseVector64F(X);
        return CommonOps.elementMin(x);
    }

    @Override
    public double zSum(final Vector X) {
        final DenseMatrix64F x = convertDVectorToDenseVector64F(X);
        return CommonOps.elementSum(x);
    }

    // ---------------------------------------------------
    // Static Methods.
    // ---------------------------------------------------

    public static DenseMatrix64F convertDVectorToDenseVector64F(final Vector vector) {
        switch (vector.getVectorType()) {
            case COLUMN_VECTOR:
                return new DenseMatrix64F((int)vector.size(), 1, true, vector.toArray());
            case ROW_VECTOR:
                return new DenseMatrix64F(1, (int)vector.size(), true, vector.toArray());
        }
        throw new IllegalStateException();
    }
}
