package de.tuberlin.pserver.math.utils;

import java.util.function.DoubleBinaryOperator;

/**
 * Only for performance tuning of compute intensive linear algebraic computations.
 * Constructs functions that return one of
 * <ul>
 * <li><tt>a + b*constant</tt>
 * <li><tt>a - b*constant</tt>
 * <li><tt>a + b/constant</tt>
 * <li><tt>a - b/constant</tt>
 * </ul>
 * <tt>a</tt> and <tt>b</tt> are variables, <tt>constant</tt> is fixed, but for performance reasons publicly accessible.
 * Intended to be passed to <tt>matrix.assign(otherMatrix,function)</tt> methods.
 */

public final class PlusMult implements DoubleBinaryOperator {

    private double multiplicator;

    public PlusMult(double multiplicator) {
        this.multiplicator = multiplicator;
    }

    /** Returns the result of the function evaluation. */
    @Override
    public double applyAsDouble(double a, double b) {
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