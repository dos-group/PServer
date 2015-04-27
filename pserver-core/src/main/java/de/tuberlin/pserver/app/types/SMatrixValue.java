package de.tuberlin.pserver.app.types;


import de.tuberlin.pserver.app.dht.Value;
import de.tuberlin.pserver.math.SMatrix;
import de.tuberlin.pserver.math.SVector;
import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import de.tuberlin.pserver.math.delegates.MathLibFactory;

public class SMatrixValue extends Value implements SMatrix {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final MathLibFactory.SMathLibrary mathLib = MathLibFactory.SMathLibrary.UJMP_LIBRARY;

    private static final LibraryMatrixOps<SMatrix, SVector> matrixOpDelegate =
            MathLibFactory.delegateSMatrixOpsTo(mathLib);

    // ---------------------------------------------------

    private final Object internalMatrix;

    protected final long rows;

    protected final long cols;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SMatrixValue(final long rows, final long cols) {
        this.rows = rows;
        this.cols = cols;
        this.internalMatrix = MathLibFactory.createSMatrixInternalObject(mathLib, this);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public long numRows() { return rows; }

    @Override
    public long numCols() { return cols; }

    @Override
    public Object getInternalMatrix() { return internalMatrix; }

    // ---------------------------------------------------
    // Matrix Operation Delegates.
    // ---------------------------------------------------

    @Override public SMatrix add(final SMatrix B) { return matrixOpDelegate.add(B, this); }

    @Override public SMatrix sub(final SMatrix B) { return matrixOpDelegate.sub(B, this); }

    @Override public SVector mul(final SVector x, final SVector y) { return matrixOpDelegate.mul(this, x, y); }

    @Override public SMatrix scale(final double alpha) { return matrixOpDelegate.scale(alpha, this); }

    @Override public SMatrix transpose() { return matrixOpDelegate.transpose(this); }

    @Override public SMatrix transpose(final SMatrix B) { return matrixOpDelegate.transpose(B, this); }

    @Override public boolean invert() { return matrixOpDelegate.invert(this); }
}
