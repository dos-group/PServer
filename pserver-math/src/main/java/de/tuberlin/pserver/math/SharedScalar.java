package de.tuberlin.pserver.math;


import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

public class SharedScalar implements SharedObject {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(SharedScalar.class);

    private final ReentrantLock lock = new ReentrantLock(true);

    private Object owner;

    private double[] data;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SharedScalar(final double value) {
        this.data = new double[1];
        this.data[0] = value;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void setValue(final double value) { data[0] = value; }

    public double getValue() { return data[0]; }

    // ---------------------------------------------------

    @Override
    public void setOwner(final Object owner) { this.owner = owner; }

    @Override
    public Object getOwner() { return this.owner; }

    @Override
    public long sizeOf() { return Double.BYTES; }

    @Override
    public double[] toArray() { return data; }

    @Override
    public void setArray(double[] data) { Preconditions.checkState(data.length == 1); this.data = data; }

    @Override
    public void lock() { lock.lock(); }

    @Override
    public void unlock() { lock.unlock(); }
}
