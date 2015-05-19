package de.tuberlin.pserver.experimental.old;

public interface Vector {

    public abstract void set(final int index, final double value);

    public abstract double get(final int index);

    public abstract Vector assign(final Vector v);

    public abstract Vector assign(final double v);

    public abstract double maxValue();

    public abstract Vector divide(final double v);

    public abstract double norm(final double v);

    public abstract Vector viewPart(final long s, final long e);
}
