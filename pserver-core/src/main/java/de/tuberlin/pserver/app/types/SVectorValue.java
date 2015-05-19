package de.tuberlin.pserver.app.types;


import de.tuberlin.pserver.app.dht.Value;
import de.tuberlin.pserver.experimental.old.SVector;
import de.tuberlin.pserver.experimental.old.delegates.LibraryVectorOps;
import de.tuberlin.pserver.experimental.old.delegates.MathLibFactory;

public class SVectorValue extends Value implements SVector {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final MathLibFactory.SMathLibrary mathLib = MathLibFactory.SMathLibrary.UJMP_LIBRARY;

    private static final LibraryVectorOps<SVector> vectorOpDelegate =
            MathLibFactory.delegateSVectorOpsTo(mathLib);

    // ---------------------------------------------------

    private final Object internalVector;

    protected final long size;

    protected final VectorType type;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SVectorValue(final long size, final VectorType type) {
        this.size = size;
        this.type = type;
        this.internalVector = MathLibFactory.createSVectorInternalObject(mathLib, this);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public long size() { return size; }

    @Override
    public Object getInternalVector() { return internalVector; }

    @Override
    public VectorType getVectorType() { return type; }

    // ---------------------------------------------------
    // Matrix Operation Delegates.
    // ---------------------------------------------------

    @Override public SVector scale(final double alpha) { return vectorOpDelegate.scale(this, alpha); }

    @Override public SVector add(final SVector y) { return vectorOpDelegate.add(this, y); }

    @Override public SVector add(final double alpha, final SVector y) { return vectorOpDelegate.add(this, alpha, y); }

    @Override public double dot(final SVector y) { return vectorOpDelegate.dot(this, y); }
}
