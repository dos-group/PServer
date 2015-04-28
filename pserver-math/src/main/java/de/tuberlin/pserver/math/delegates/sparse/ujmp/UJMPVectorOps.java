package de.tuberlin.pserver.math.delegates.sparse.ujmp;


import de.tuberlin.pserver.math.SVector;
import de.tuberlin.pserver.math.delegates.LibraryVectorOps;
import org.ujmp.core.matrix.SparseMatrix;

public final class UJMPVectorOps implements LibraryVectorOps<SVector> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public SVector scale(final SVector x, final double alpha) {
        return null;
    }

    @Override
    public SVector add(final SVector x, final SVector y) {
        return null;
    }

    @Override
    public SVector add(final SVector x, final double alpha, final SVector y) {
        return null;
    }

    @Override
    public double dot(final SVector x, final SVector y) {
        return 0;
    }

    // ---------------------------------------------------
    // Static Methods.
    // ---------------------------------------------------

    private static SparseMatrix convertSVectorToUJMPSparseVector(final SVector vector) {
        return (SparseMatrix)vector.getInternalVector();
    }
}
