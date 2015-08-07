package de.tuberlin.pserver.math.utils;

import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

public final class Functions {

    private Functions() {}

    public static final DoubleUnaryOperator ABS = a -> Math.abs(a);

    public static final DoubleUnaryOperator ACOS = a -> Math.acos(a);

    public static final DoubleUnaryOperator ASIN = a -> Math.asin(a);

    public static final DoubleUnaryOperator ATAN = a -> Math.atan(a);

    public static final DoubleUnaryOperator CEIL = a -> Math.ceil(a);

    public static final DoubleUnaryOperator COS = a -> Math.cos(a);

    public static final DoubleUnaryOperator EXP = a -> Math.exp(a);

    public static final DoubleUnaryOperator FLOOR = a -> Math.floor(a);

    public static final DoubleUnaryOperator IDENTITY = a -> a;

    public static final DoubleUnaryOperator INV = a -> 1.0 / a;

    public static final DoubleUnaryOperator LOGARITHM = a -> Math.log(a);

    public static final DoubleUnaryOperator LOG2 = a -> Math.log(a) * 1.4426950408889634;

    public static final DoubleUnaryOperator NEGATE = a -> -a;

    public static final DoubleUnaryOperator RINT = a -> Math.rint(a);

    public static final DoubleUnaryOperator SIGN = a -> a < 0 ? -1 : a > 0 ? 1 : 0;

    public static final DoubleUnaryOperator SIN = a -> Math.sin(a);

    public static final DoubleUnaryOperator SQRT = a -> Math.sqrt(a);

    public static final DoubleUnaryOperator SQUARE = a -> a * a;

    public static final DoubleUnaryOperator SIGMOID = a -> 1.0 / (1.0 + Math.exp(-a));

    public static final DoubleUnaryOperator SIGMOIDGRADIENT = a -> a * (1.0 - a);

    public static final DoubleUnaryOperator TAN = a -> Math.tan(a);

    public static final DoubleBinaryOperator ATAN2 = (a, b) -> Math.atan2(a, b);

    public static final DoubleBinaryOperator COMPARE = (a, b) -> a < b ? -1 : a > b ? 1 : 0;

    public static final DoubleBinaryOperator DIV = (a, b) -> a / b;

    public static final DoubleBinaryOperator EQUALS = (a, b) -> a == b ? 1 : 0;

    public static final DoubleBinaryOperator GREATER = (a, b) -> a > b ? 1 : 0;

    public static final DoubleBinaryOperator IEEE_REMAINDER = (a, b) -> Math.IEEEremainder(a, b);

    public static final DoubleBinaryOperator LESS = (a, b) -> a < b ? 1 : 0;

    public static final DoubleBinaryOperator LG = (a, b) -> Math.log(a) / Math.log(b);

    public static final DoubleBinaryOperator MAX = (a, b) -> Math.max(a, b);

    public static final DoubleBinaryOperator MIN = (a, b) -> Math.min(a, b);

    public static final DoubleBinaryOperator MINUS = plusMult(-1);

    public static final DoubleBinaryOperator MOD = (a, b) -> a % b;

    public static final DoubleBinaryOperator MULT = (a, b) -> a * b;

    public static final DoubleBinaryOperator PLUS = (a, b) -> a + b;

    public static final DoubleBinaryOperator PLUS_ABS = (a, b) -> Math.abs(a) + Math.abs(b);

    public static final DoubleBinaryOperator POW = (a, b) -> Math.pow(a, b);

    public static DoubleUnaryOperator between(final double from, final double to) { return a -> from <= a && a <= to ? 1 : 0; }

    public static DoubleUnaryOperator bindArg1(final DoubleBinaryOperator function, final double c) { return var -> function.applyAsDouble(c, var); }

    public static DoubleUnaryOperator bindArg2(final DoubleBinaryOperator function, final double c) { return var -> function.applyAsDouble(var, c); }

    public static DoubleBinaryOperator chain(final DoubleBinaryOperator f, final DoubleUnaryOperator g,
                                             final DoubleUnaryOperator h) {
        return new DoubleBinaryOperator() {

            @Override
            public double applyAsDouble(double a, double b) {
                return f.applyAsDouble(g.applyAsDouble(a), h.applyAsDouble(b));
            }
        };
    }

    public static DoubleBinaryOperator chain(final DoubleUnaryOperator g, final DoubleBinaryOperator h) {
        return new DoubleBinaryOperator() {

            @Override
            public double applyAsDouble(double a, double b) {
                return g.applyAsDouble(h.applyAsDouble(a, b));
            }
        };
    }

    public static DoubleUnaryOperator chain(final DoubleUnaryOperator g, final DoubleUnaryOperator h) {
        return new DoubleUnaryOperator() {

            @Override
            public double applyAsDouble(double a) {
                return g.applyAsDouble(h.applyAsDouble(a));
            }
        };
    }

    public static DoubleUnaryOperator compare(final double b) { return a -> a < b ? -1 : a > b ? 1 : 0; }

    public static DoubleUnaryOperator constant(final double c) { return a -> c; }

    public static DoubleUnaryOperator equals(final double b) { return a -> a == b ? 1 : 0; }

    public static DoubleUnaryOperator greater(final double b) { return a -> a > b ? 1 : 0; }

    public static DoubleUnaryOperator mathIEEEremainder(final double b) { return a -> Math.IEEEremainder(a, b); }

    public static DoubleUnaryOperator less(final double b) { return a -> a < b ? 1 : 0; }

    public static DoubleUnaryOperator lg(final double b) {
        return new DoubleUnaryOperator() {
            private final double logInv = 1 / Math.log(b); // cached for speed


            @Override
            public double applyAsDouble(double a) {
                return Math.log(a) * logInv;
            }
        };
    }

    public static DoubleUnaryOperator max(final double b) { return a -> Math.max(a, b); }

    public static DoubleUnaryOperator min(final double b) { return a -> Math.min(a, b); }

    public static DoubleUnaryOperator minus(double b) { return plus(-b); }

    public static DoubleBinaryOperator minusMult(double constant) { return plusMult(-constant); }

    public static DoubleUnaryOperator mod(final double b) { return a -> a % b; }

    public static DoubleUnaryOperator plus(final double b) { return a -> a + b; }

    public static DoubleBinaryOperator plusMult(double constant) { return new PlusMult(constant); }

    public static DoubleUnaryOperator pow(final double b) { return a -> Math.pow(a, b); }

    public static DoubleUnaryOperator round(final double precision) { return a -> Math.rint(a / precision) * precision; }

    public static DoubleBinaryOperator swapArgs(final DoubleBinaryOperator function) {
        return new DoubleBinaryOperator() {
            @Override
            public double applyAsDouble(double a, double b) {
                return function.applyAsDouble(b, a);
            }
        };
    }
}