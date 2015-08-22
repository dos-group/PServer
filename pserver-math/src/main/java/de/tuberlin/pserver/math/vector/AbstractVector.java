package de.tuberlin.pserver.math.vector;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.utils.Utils;

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

    protected abstract Vector newInstance(long length);

    // -----------------------------------------------------------------------------------------------------------------
    // Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public Vector mul(double alpha) {
        return mul(alpha, newInstance(length));
    }

    @Override
    public Vector mul(double alpha, final Vector y) {
        for(long i = 0; i < length; i++) {
            y.set(i, get(i) * alpha);
        }
        return this;
    }

    @Override
    public Vector div(double alpha) {
        return div(alpha, newInstance(length));
    }

    @Override
    public Vector div(double alpha, final Vector y) {
        for(long i = 0; i < length; i++) {
            y.set(i, get(i) / alpha);
        }
        return this;
    }

    @Override
    public Vector add(Vector y) {
        return add(1, y, newInstance(length));
    }

    @Override
    public Vector add(Vector y, Vector z) {
        return add(1, y, z);
    }

    @Override
    public Vector sub(Vector y) {
        return add(-1, y, newInstance(length));
    }

    @Override
     public Vector sub(Vector y, Vector z) {
        return add(-1, y, z);
    }

    @Override
    public Vector add(double alpha, Vector y) {
        return add(alpha, y, newInstance(length));
    }

    @Override
    public Vector add(double alpha, Vector y, Vector z) {
        Utils.checkShapeEqual(this, y);
        for(long i = 0; i < length; i++) {
            z.set(i, alpha * y.get(i) + get(i));
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
        Utils.checkShapeEqual(this, v);
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
    public double aggregate(DoubleBinaryOperator aggregator, DoubleUnaryOperator map) {
        double result = 0;
        for(long i = 0; i < length; i++) {
            result += aggregator.applyAsDouble(result, map.applyAsDouble(get(i)));
        }
        return result;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ApplyOnDoubleElements
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public Vector applyOnElements(final DoubleUnaryOperator f) {
        return applyOnElements(f, newInstance(length));
    }

    @Override
    public Vector applyOnElements(final DoubleUnaryOperator f, final Vector B) {
        Utils.checkShapeEqual(this, B);
        for (int i = 0; i < B.length(); ++i) {
            B.set(i, f.applyAsDouble(B.get(i)));
        }
        return B;
    }

    @Override
    public Vector applyOnElements(final Vector B, final DoubleBinaryOperator f) {
        return applyOnElements(B, f, newInstance(length));
    }

    @Override
    public Vector applyOnElements(final Vector B, final DoubleBinaryOperator f, final Vector C) {
        Utils.checkShapeEqual(this, B, C);
        for (int i = 0; i < C.length(); ++i) {
            C.set(i, f.applyAsDouble(this.get(i), B.get(i)));
        }
        return C;
    }
}
