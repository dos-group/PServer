package de.tuberlin.pserver.math.utils;

public final class PlusMult implements DoubleFunction2Arg {

    private double multiplicator;

    public PlusMult(double multiplicator) {
        this.multiplicator = multiplicator;
    }

    /** Returns the result of the function evaluation. */
    @Override
    public double apply(double a, double b) {
        return a + b * multiplicator;
    }

    /** <tt>a - b*constant</tt>. */
    public static PlusMult minusMult(double constant) {
        return new PlusMult(-constant);
    }

    /** <tt>a + b*constant</tt>. */
    public static PlusMult plusMult(double constant) {
        return new PlusMult(constant);
    }

    public double getMultiplicator() {
        return multiplicator;
    }

    public void setMultiplicator(double multiplicator) {
        this.multiplicator = multiplicator;
    }
}