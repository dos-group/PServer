package de.tuberlin.pserver.math.delegates.sparse.mtj;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.*;
import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import de.tuberlin.pserver.math.delegates.LibraryVectorOps;
import no.uib.cipr.matrix.*;
import no.uib.cipr.matrix.sparse.*;

public class MTJVectorOps implements LibraryVectorOps<Vector> {


    @Override
    public Vector mul(Vector x, double alpha) {
        return MTJUtils.toPserverVector(MTJUtils.toLibVector(x).scale(alpha), x.getVectorType());
    }

    @Override
    public Vector div(Vector x, double alpha) {
        return MTJUtils.toPserverVector(MTJUtils.toLibVector(x).scale(1. / alpha), x.getVectorType());
    }

    @Override
    public Vector add(Vector x, Vector y) {
        return MTJUtils.toPserverVector(MTJUtils.toLibVector(x).add(MTJUtils.toLibVector(y)), x.getVectorType());
    }

    @Override
    public Vector sub(Vector x, Vector y) {
        return MTJUtils.toPserverVector(MTJUtils.toLibVector(x).scale(-1.).add(MTJUtils.toLibVector(y)), x.getVectorType());
    }

    @Override
    public Vector add(Vector x, double alpha, Vector y) {
        return null;
    }

    @Override
    public double dot(Vector x, Vector y) {
        return MTJUtils.toLibVector(x).dot(MTJUtils.toLibVector(y));
    }

    @Override
    public double norm(Vector x, double power) {
        no.uib.cipr.matrix.Vector.Norm norm;
        if(power == 1) {
            norm = no.uib.cipr.matrix.Vector.Norm.One;
        }
        else if(power == 2) {
            norm = no.uib.cipr.matrix.Vector.Norm.Two;
        }
        else {
            throw new UnsupportedOperationException("MTJ only supports L1 and L2 norms.");
        }
        return MTJUtils.toLibVector(x).norm(norm);
    }

    @Override
    public double maxValue(Vector x) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double minValue(Vector x) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double zSum(Vector x) {
        throw new UnsupportedOperationException();
    }
}
