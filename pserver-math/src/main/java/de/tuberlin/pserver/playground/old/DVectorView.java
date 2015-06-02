package de.tuberlin.pserver.playground.old;

// TODO:
public class DVectorView implements DVector {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int offset;

    private final int size;

    private final DVector delegate;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DVectorView(final int offset, final int size, final DVector delegate) {
        this.offset     = offset;
        this.size       = size;
        this.delegate   = delegate;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public int size() {
        return 0;
    }

    @Override
    public void set(int index, double value) {

    }

    @Override
    public double get(int index) {
        return 0;
    }

    @Override
    public DVector zero() {
        return null;
    }

    @Override
    public DVector set(DVector y) {
        return null;
    }

    @Override
    public VectorType getVectorType() {
        return null;
    }

    @Override
    public double[] toArray() {
        return new double[0];
    }

    @Override
    public void setArray(double[] data) {}

    // ---------------------------------------------------
    // Vector Operation Delegates.
    // ---------------------------------------------------

    @Override
    public DVector scale(double alpha) {
        return null;
    }

    @Override
    public DVector add(DVector y) {
        return null;
    }

    @Override
    public DVector add(double alpha, DVector y) {
        return null;
    }

    @Override
    public double dot(DVector y) {
        return 0;
    }
}
