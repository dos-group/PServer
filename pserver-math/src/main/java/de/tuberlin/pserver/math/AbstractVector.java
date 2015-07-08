package de.tuberlin.pserver.math;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.stuff.Utils;

import java.util.Iterator;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

/**
 * An abstract implementation of the Vector interface, that contains default implementations for
 * any method that is expressible by the interface itself. This implementation is agnostic to
 * underlying data structures. However, the efficiency of operations is largely dependent on
 * exploitations of the underlying data structure's properties. Thus, overwriting these methods is still
 * recommended.
 */
public abstract class AbstractVector implements Vector {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected long length;

    protected Layout type;

    protected Object owner;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractVector(long length, Layout type) {
        this.length = length;
        this.type = type;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public long sizeOf() {
        return length * Double.BYTES;
    }

    @Override
    public long length() { return length; }

    @Override
    public Layout layout() {
        return type;
    }

    @Override
    public void setOwner(final Object owner) { this.owner = owner; }

    @Override
    public Object getOwner() { return owner; }

    @Override
    public double[] toArray() {
        double[] result = new double[Utils.toInt(length)];
        for(long i = 0; i < length; i++) {
            result[Utils.toInt(i)] = get(i);
        }
        return result;
    }

    @Override
    public void setArray(double[] data) {
        Preconditions.checkArgument(data.length == length,
                String.format("Can not set array of Vector because length of " +
                              "array (%d) is not equal to length of Vector (%d)", data.length, length));
        for(int i = 0; i < length; i++) {
            set(i, data[i]);
        }
    }

    @Override
    public Vector mul(double alpha) {
        for(long i = 0; i < length; i++) {
            set(i, get(i) * alpha);
        }
        return this;
    }

    @Override
    public Vector div(double alpha) {
        for(long i = 0; i < length; i++) {
            set(i, get(i) / alpha);
        }
        return this;
    }

    @Override
    public Vector add(Vector y) {
        return add(1, y);
    }

    @Override
    public Vector sub(Vector y) {
        return add(-1, y);
    }

    @Override
    public Vector add(double alpha, Vector y) {
        checkDimensions(y);
        for(long i = 0; i < length; i++) {
            set(i, alpha * y.get(i) + get(i));
        }
        return this;
    }

    @Override
    public double dot(Vector y) {
        double result = 0;
        for(long i = 0; i < length; i++) {
            result += get(i) * y.get(i);
        }
        return result;
    }

    @Override
    public double sum() {
        double result = 0;
        for(long i = 0; i < length; i++) {
            result += get(i);
        }
        return result;
    }

    @Override
    public double norm(double v) {
        double result = 0;
        for(long i = 0; i < length; i++) {
            result += Math.pow(get(i), 2);
        }
        return Math.sqrt(result);
    }

    @Override
    public double maxValue() {
        double result = Double.MIN_VALUE;
        for(long i = 0; i < length; i++) {
            result = Math.max(result, get(i));
        }
        return result;
    }

    @Override
    public double minValue() {
        double result = Double.MAX_VALUE;
        for(long i = 0; i < length; i++) {
            result = Math.min(result, get(i));
        }
        return result;
    }

    @Override
    public Vector assign(Vector v) {
        checkDimensions(v);
        for(long i = 0; i < length; i++) {
            set(i, v.get(i));
        }
        return this;
    }

    @Override
    public Vector assign(double v) {
        for(long i = 0; i < length; i++) {
            set(i, v);
        }
        return this;
    }

    @Override
    public Vector assign(DoubleUnaryOperator df) {
        for(long i = 0; i < length; i++) {
            set(i, df.applyAsDouble(get(i)));
        }
        return this;
    }

    @Override
    public Vector assign(Vector v, DoubleBinaryOperator df) {
        for(long i = 0; i < length; i++) {
            set(i, df.applyAsDouble(get(i), v.get(i)));
        }
        return this;
    }

    @Override
    public Iterator<Element> iterateNonZero() {
        return new DefaultIterator(this);
    }

    @Override
    public double aggregate(DoubleBinaryOperator aggregator, DoubleUnaryOperator map) {
        double result = 0;
        for(long i = 0; i < length; i++) {
            result += aggregator.applyAsDouble(result, map.applyAsDouble(get(i)));
        }
        return result;
    }

    protected void checkDimensions(Vector arg) {
        Preconditions.checkArgument(length == arg.length(), String.format("Can not apply operation because supplied vector length (%d) differs from base vector length (%d)",arg.length(), length));
    }

    @Override
    public Vector applyOnElements(final VectorFunction1Arg vf) {
        final Vector res = copy();
        for (int i = 0; i < res.length(); ++i) {
            res.set(i, vf.operation(this.get(i)));
        }
        return res;
    }

    @Override
    public Vector applyOnElements(final Vector v2, final VectorFunction1Arg vf) {
        final Vector res = copy();
        for (int i = 0; i < res.length(); ++i) {
            res.set(i, vf.operation(v2.get(i)));
        }
        return res;
    }

    @Override
    public Vector applyOnElements(final Vector v2, final VectorFunction2Arg vf) {
        final Vector res = copy();
        Preconditions.checkState(v2.length() == res.length());
        for (int i = 0; i < res.length(); ++i) {
            res.set(i, vf.operation(this.get(i), v2.get(i)));
        }
        return res;
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public class DefaultIterator implements Iterator<Element> {

        private long currentIndex = 0;
        private final Vector vector;

        public DefaultIterator(Vector vector) {
            this.vector = vector;
        }

        @Override
        public boolean hasNext() {
            return currentIndex < vector.length();
        }

        @Override
        public Element next() {
            Preconditions.checkState(hasNext(), "Iterator has no more elements.");
            return new DefaultElement(vector, currentIndex++);
        }
    }

    public class DefaultElement implements Element {

        private final Vector vector;
        private final long index;

        public DefaultElement(Vector vector, long index) {
            this.vector = vector;
            this.index = index;
        }

        @Override
        public double get() {
            return vector.get(index);
        }

        @Override
        public int index() {
            return Utils.toInt(index);
        }

        @Override
        public void set(double value) {
            vector.set(index, value);
        }
    }
}
