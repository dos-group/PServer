package de.tuberlin.pserver.math.vector.dense;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.delegates.LibraryVectorOps;
import de.tuberlin.pserver.math.delegates.MathLibFactory;
import de.tuberlin.pserver.math.utils.Utils;
import de.tuberlin.pserver.math.vector.AbstractVector;
import de.tuberlin.pserver.math.vector.Vector;

import java.util.Arrays;
import java.util.List;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

public class DVector extends AbstractVector {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Object owner;

    private static final LibraryVectorOps<Vector> vectorOpDelegate =
            MathLibFactory.delegateDVectorOpsTo(MathLibFactory.DMathLibrary.EJML_LIBRARY);

    protected double[] data;

    protected Layout type;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    // Copy Constructor.
    public DVector(final DVector v) { this(v, v.type); }
    public DVector(final DVector v, final Layout type) {
        super(v.data.length, type);
        this.data = new double[v.data.length];
        System.arraycopy(v.data, 0, this.data, 0, v.data.length);
        this.type = type;
    }

    public DVector(final long size) { this(size, null, Layout.ROW_LAYOUT); }
    public DVector(final long size, final Layout type) { this(size, null, type); }
    public DVector(final long size, final double[] data) { this(size, data, Layout.ROW_LAYOUT); }
    public DVector(final long size, final double[] data, final Layout type) {
        super(size, type);
        this.data = (data == null) ? new double[(int)size] : data;
        this.type = Preconditions.checkNotNull(type);
    }

    // ---------------------------------------------------
    // Matrix Operation Delegates.
    // ---------------------------------------------------

    @Override public Vector mul(final double alpha, final Vector y) { return vectorOpDelegate.mul(this, alpha, y); }

    @Override public Vector div(final double alpha, final Vector y) { return vectorOpDelegate.div(this, alpha, y); }

    @Override public Vector add(final Vector y, final Vector z) { return vectorOpDelegate.add(this, y, z); }

    @Override public Vector sub(final Vector y, final Vector z) { return vectorOpDelegate.sub(this, y, z); }

    @Override public Vector add(final double alpha, final Vector y, final Vector z) { return vectorOpDelegate.add(this, alpha, y, z); }

    @Override public double dot(final Vector y) { return vectorOpDelegate.dot(this, y); }

    @Override public double norm(final double power) { return vectorOpDelegate.norm(this, power); }

    @Override public double maxValue() { return vectorOpDelegate.maxValue(this); }

    @Override public double minValue() { return vectorOpDelegate.minValue(this); }

    @Override public double sum() { return vectorOpDelegate.zSum(this); }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override public long sizeOf() { return data.length * Double.BYTES; }

    // -----------------------------------------------------------------------------------------------------------------
    // ApplyOnDoubleElements
    // -----------------------------------------------------------------------------------------------------------------


    @Override
    protected Vector newInstance(long length) {
        return new DVector(length);
    }

    @Override
    public Vector applyOnElements(final DoubleUnaryOperator f, final Vector B) {
        Utils.checkShapeEqual(this, B);
        int length = (int) length();
        final double[] result = B.toArray();
        for (int i = 0; i < length; ++i) {
            result[i] = f.applyAsDouble(data[i]);
        }
        return B;
    }

    @Override
    public Vector applyOnElements(final Vector B, final DoubleBinaryOperator f, final Vector C) {
        Utils.checkShapeEqual(this, B, C);
        int length = data.length;
        final double[] bData = B.toArray();
        final double[] result = C.toArray();
        for (int i = 0; i < length; ++i) {
            result[i] = f.applyAsDouble(data[i], bData[i]);
        }
        return C;
    }

    @Override public long length() { return data.length; }

    @Override public Layout layout() { return type; }

    @Override public Format format() { return Format.DENSE_FORMAT; }

    @Override public void setOwner(final Object owner) { this.owner = owner; }

    @Override public Object getOwner() { return owner; }

    @Override public void set(final long index, final double value) { data[(int)index] = value; }

    @Override public double get(long index) { return data[(int)index]; }

    @Override public double[] toArray() { return data; }

    @Override public void setArray(final double[] data) { this.data = Preconditions.checkNotNull(data); }

    @Override public Vector like() { return new DVector(data.length, null, type); }

    @Override
    public Vector assign(final double value) {
        //this.lengthSquared = -1;
        Arrays.fill(data, value);
        return this;
    }

    @Override
    public Vector assign(final Vector vector) {
        if (vector instanceof DVector) {
            final DVector v = (DVector) vector;
            // make sure the data field has the correct length
            if (v.data.length != this.data.length)
                this.data = new double[v.data.length];
            // now copy the values
            System.arraycopy(v.data, 0, this.data, 0, this.data.length);
            return this;
        } else
            throw new IllegalStateException();
    }

    @Override
    public ElementIterator nonZeroElementIterator() { return new DVectorElementIterator(this, 0, data.length - 1, true); }

    @Override
    public ElementIterator elementIterator() { return new DVectorElementIterator(this, 0, data.length - 1, false); }

    @Override
    public ElementIterator elementIterator(int start, int end) { return new DVectorElementIterator(this, start, end, false); }

    @Override
    public double aggregate(final DoubleBinaryOperator aggregator, final DoubleUnaryOperator map) {
        Preconditions.checkArgument(data.length >= 1);
        double result = map.applyAsDouble(get(0));
        for (int i = 1; i < data.length; i++)
            result = aggregator.applyAsDouble(result, map.applyAsDouble(get(i)));
        return result;
    }

    @Override
    public Vector copy() { return new DVector(this); }

    @Override
    public Vector concat(final Vector v) {
        Preconditions.checkState(v instanceof DVector && v.layout() == layout());
        final double[] newData = new double[data.length + v.toArray().length];
        System.arraycopy(data, 0, newData, 0, data.length);
        System.arraycopy(v.toArray(), 0, newData, data.length, v.toArray().length);
        return new DVector(data.length + v.toArray().length, newData, layout());
    }

    @Override
    public Vector concat(final List<Vector> vList) {
        final long length = vList.stream()
                .map(v -> {
                    Preconditions.checkState(v instanceof DVector && v.layout() == layout());
                    return v.length();
                }).mapToLong(Long::longValue).sum();

        final double[] newData = new double[(int)length];
        int offset = 0;
        for (int i = 0; i < vList.size(); ++i) {
            final Vector v = vList.get(0);
            System.arraycopy(v.toArray(), 0, newData, offset, v.toArray().length);
            offset += v.length();
        }
        return new DVector(length, newData, layout());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (double aData : data) sb.append(aData).append(",");
        sb.deleteCharAt(sb.length() - 1);
        sb.append("]");
        if (type == Layout.COLUMN_LAYOUT)
            sb.append("^T");
        return sb.toString();
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class DVectorElementIterator implements ElementIterator {

        private final DVector self;

        private final int end;

        private final int start;

        private final boolean nonZero;

        private int index;

        public DVectorElementIterator(final DVector self, final int start, final int end, final boolean nonZero) {
            this.self    = Preconditions.checkNotNull(self);
            this.start   = start;
            this.end     = end;
            this.nonZero = nonZero;
            this.index   = start - 1;
        }

        @Override public boolean hasNextElement() { return index < end; }

        @Override public void nextElement() { if (nonZero) while(self.data[++index] == 0.0) {} else ++index; }

        @Override public void nextRandomElement() { throw new UnsupportedOperationException(); }

        @Override public double value() { return self.data[index]; }

        @Override public void reset() { index = start - 1; }

        @Override public long length() { return self.length(); }

        @Override public int getCurrentElementNum() { return index; }
    }
}
