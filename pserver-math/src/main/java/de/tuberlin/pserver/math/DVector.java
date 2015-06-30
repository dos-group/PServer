package de.tuberlin.pserver.math;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import de.tuberlin.pserver.math.delegates.LibraryVectorOps;
import de.tuberlin.pserver.math.delegates.MathLibFactory;
import de.tuberlin.pserver.math.stuff.DoubleDoubleFunction;
import de.tuberlin.pserver.math.stuff.DoubleFunction;
import de.tuberlin.pserver.math.stuff.PlusMult;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

public class DVector implements Vector, Serializable {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class DenseElement implements Element {

        int index;

        @Override public double get() { return data[index]; }

        @Override public int index() { return index; }

        @Override public void set(double value) { data[index] = value; }
    }

    // ---------------------------------------------------

    private final class NonDefaultIterator extends AbstractIterator<Element> {

        private final DenseElement element = new DenseElement();

        private int index = 0;

        @Override
        protected Element computeNext() {
            while (index < length() && data[index] == 0.0) {
                index++;
            }
            if (index < length()) {
                element.index = index;
                index++;
                return element;
            } else {
                return endOfData();
            }
        }
    }

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
        this.data = new double[v.data.length];
        System.arraycopy(v.data, 0, this.data, 0, v.data.length);
        this.type = type;
    }

    public DVector(final long size) { this(size, null, Layout.ROW_LAYOUT); }
    public DVector(final long size, final Layout type) { this(size, null, type); }
    public DVector(final long size, final double[] data) { this(size, data, Layout.ROW_LAYOUT); }
    public DVector(final long size, final double[] data, final Layout type) {
        this.data = (data == null) ? new double[(int)size] : data;
        this.type = Preconditions.checkNotNull(type);
    }

    // ---------------------------------------------------
    // Matrix Operation Delegates.
    // ---------------------------------------------------

    @Override public Vector mul(final double alpha) { return vectorOpDelegate.mul(this, alpha); }

    @Override public Vector div(final double alpha) { return vectorOpDelegate.div(this, alpha); }

    @Override public Vector add(final Vector y) { return vectorOpDelegate.add(this, y); }

    @Override public Vector sub(final Vector y) { return vectorOpDelegate.sub(this, y); }

    @Override public Vector add(final double alpha, final Vector y) { return vectorOpDelegate.add(this, alpha, y); }

    @Override public double dot(final Vector y) { return vectorOpDelegate.dot(this, y); }

    @Override public double norm(final double power) { return vectorOpDelegate.norm(this, power); }

    @Override public double maxValue() { return vectorOpDelegate.maxValue(this); }

    @Override public double minValue() { return vectorOpDelegate.minValue(this); }

    @Override public double zSum() { return vectorOpDelegate.zSum(this); }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override public long sizeOf() { return data.length * Double.BYTES; }

    @Override public long length() { return data.length; }

    @Override public Layout layout() { return type; }

    @Override public Format format() { return Format.DENSE_VECTOR; }

    @Override public void setOwner(final Object owner) { this.owner = owner; }

    @Override public Object getOwner() { return owner; }

    @Override public void set(final long index, final double value) { data[(int)index] = value; }

    @Override public double get(long index) { return data[(int)index]; }

    @Override public double atomicGet(final long index) { throw new UnsupportedOperationException(); }

    @Override public void atomicSet(final long index, final double value) { throw new UnsupportedOperationException(); }

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
    public Vector assign(final DoubleFunction df) {
        for (int i = 0; i < length(); i++) {
            data[i] = df.apply(data[i]);
        }
        return this;
    }

    @Override
    public Vector assign(final Vector other, final DoubleDoubleFunction function) {
        Preconditions.checkState(length() == other.length());
        // is there some other way to know if function.apply(0, x) = x for all x?
        if (function instanceof PlusMult) {
            Iterator<Element> it = other.iterateNonZero();
            Element e;
            while (it.hasNext() && (e = it.next()) != null) {
                data[e.index()] = function.apply(data[e.index()], e.get());
            }
        } else {
            for (int i = 0; i < length(); i++) {
                data[i] = function.apply(data[i], other.get(i));
            }
        }
        //lengthSquared = -1;
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
    public Iterator<Element> iterateNonZero() {
        return new NonDefaultIterator();
    }

    @Override
    public double aggregate(final DoubleDoubleFunction aggregator, final DoubleFunction map) {
        Preconditions.checkArgument(data.length >= 1);
        double result = map.apply(get(0));
        for (int i = 1; i < data.length; i++)
            result = aggregator.apply(result, map.apply(get(i)));
        return result;
    }

    @Override
    public Vector copy() {
        return new DVector(this);
    }

    @Override
    public Vector viewPart(final long s, final long e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < data.length; i++)
            sb.append(data[i]).append(",");
        sb.deleteCharAt(sb.length() - 1);
        sb.append("]");
        if (type == Layout.COLUMN_LAYOUT)
            sb.append("^T");
        return sb.toString();
    }
}
