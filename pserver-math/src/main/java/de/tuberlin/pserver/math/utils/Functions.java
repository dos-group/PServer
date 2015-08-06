package de.tuberlin.pserver.math.utils;

public final class Functions {

    private Functions() {}

    public static final DoubleFunction1Arg ABS = a -> Math.abs(a);

    public static final DoubleFunction1Arg ACOS = a -> Math.acos(a);

    public static final DoubleFunction1Arg ASIN = a -> Math.asin(a);

    public static final DoubleFunction1Arg ATAN = a -> Math.atan(a);

    public static final DoubleFunction1Arg CEIL = a -> Math.ceil(a);

    public static final DoubleFunction1Arg COS = a -> Math.cos(a);

    public static final DoubleFunction1Arg EXP = a -> Math.exp(a);

    public static final DoubleFunction1Arg FLOOR = a -> Math.floor(a);

    public static final DoubleFunction1Arg IDENTITY = a -> a;

    public static final DoubleFunction1Arg INV = a -> 1.0 / a;

    public static final DoubleFunction1Arg LOGARITHM = a -> Math.log(a);

    public static final DoubleFunction1Arg LOG2 = a -> Math.log(a) * 1.4426950408889634;

    public static final DoubleFunction1Arg NEGATE = a -> -a;

    public static final DoubleFunction1Arg RINT = a -> Math.rint(a);

    public static final DoubleFunction1Arg SIGN = a -> a < 0 ? -1 : a > 0 ? 1 : 0;

    public static final DoubleFunction1Arg SIN = a -> Math.sin(a);

    public static final DoubleFunction1Arg SQRT = a -> Math.sqrt(a);

    public static final DoubleFunction1Arg SQUARE = a -> a * a;

    public static final DoubleFunction1Arg SIGMOID = a -> 1.0 / (1.0 + Math.exp(-a));

    public static final DoubleFunction1Arg SIGMOIDGRADIENT = a -> a * (1.0 - a);

    public static final DoubleFunction1Arg TAN = a -> Math.tan(a);

    public static final DoubleFunction2Arg ATAN2 = (a, b) -> Math.atan2(a, b);

    public static final DoubleFunction2Arg COMPARE = (a, b) -> a < b ? -1 : a > b ? 1 : 0;

    public static final DoubleFunction2Arg DIV = (a, b) -> a / b;

    public static final DoubleFunction2Arg EQUALS = (a, b) -> a == b ? 1 : 0;

    public static final DoubleFunction2Arg GREATER = (a, b) -> a > b ? 1 : 0;

    public static final DoubleFunction2Arg IEEE_REMAINDER = (a, b) -> Math.IEEEremainder(a, b);

    public static final DoubleFunction2Arg LESS = (a, b) -> a < b ? 1 : 0;

    public static final DoubleFunction2Arg LG = (a, b) -> Math.log(a) / Math.log(b);

    public static final DoubleFunction2Arg MAX = (a, b) -> Math.max(a, b);

    public static final DoubleFunction2Arg MIN = (a, b) -> Math.min(a, b);

    public static final DoubleFunction2Arg MINUS = plusMult(-1);

    public static final DoubleFunction2Arg MOD = (a, b) -> a % b;

    public static final DoubleFunction2Arg MULT = (a, b) -> a * b;

    public static final DoubleFunction2Arg PLUS = (a, b) -> a + b;

    public static final DoubleFunction2Arg PLUS_ABS = (a, b) -> Math.abs(a) + Math.abs(b);

    public static final DoubleFunction2Arg POW = (a, b) -> Math.pow(a, b);

    public static DoubleFunction1Arg between(final double from, final double to) { return a -> from <= a && a <= to ? 1 : 0; }

    public static DoubleFunction1Arg bindArg1(final DoubleFunction2Arg function, final double c) { return var -> function.apply(c, var); }

    public static DoubleFunction1Arg bindArg2(final DoubleFunction2Arg function, final double c) { return var -> function.apply(var, c); }

    public static DoubleFunction2Arg chain(final DoubleFunction2Arg f, final DoubleFunction1Arg g,
                                             final DoubleFunction1Arg h) {
        return new DoubleFunction2Arg() {

            @Override
            public double apply(double a, double b) {
                return f.apply(g.apply(a), h.apply(b));
            }
        };
    }

    public static DoubleFunction2Arg chain(final DoubleFunction1Arg g, final DoubleFunction2Arg h) {
        return new DoubleFunction2Arg() {

            @Override
            public double apply(double a, double b) {
                return g.apply(h.apply(a, b));
            }
        };
    }

    public static DoubleFunction1Arg chain(final DoubleFunction1Arg g, final DoubleFunction1Arg h) {
        return new DoubleFunction1Arg() {

            @Override
            public double apply(double a) {
                return g.apply(h.apply(a));
            }
        };
    }

    public static DoubleFunction1Arg compare(final double b) { return a -> a < b ? -1 : a > b ? 1 : 0; }

    public static DoubleFunction1Arg constant(final double c) { return a -> c; }

    public static DoubleFunction1Arg equals(final double b) { return a -> a == b ? 1 : 0; }

    public static DoubleFunction1Arg greater(final double b) { return a -> a > b ? 1 : 0; }

    public static DoubleFunction1Arg mathIEEEremainder(final double b) { return a -> Math.IEEEremainder(a, b); }

    public static DoubleFunction1Arg less(final double b) { return a -> a < b ? 1 : 0; }

    public static DoubleFunction1Arg lg(final double b) {
        return new DoubleFunction1Arg() {
            private final double logInv = 1 / Math.log(b); // cached for speed


            @Override
            public double apply(double a) {
                return Math.log(a) * logInv;
            }
        };
    }

    public static DoubleFunction1Arg max(final double b) { return a -> Math.max(a, b); }

    public static DoubleFunction1Arg min(final double b) { return a -> Math.min(a, b); }

    public static DoubleFunction1Arg minus(double b) { return plus(-b); }

    public static DoubleFunction2Arg minusMult(double constant) { return plusMult(-constant); }

    public static DoubleFunction1Arg mod(final double b) { return a -> a % b; }

    public static DoubleFunction1Arg plus(final double b) { return a -> a + b; }

    public static DoubleFunction2Arg plusMult(double constant) { return new PlusMult(constant); }

    public static DoubleFunction1Arg pow(final double b) { return a -> Math.pow(a, b); }

    public static DoubleFunction1Arg round(final double precision) { return a -> Math.rint(a / precision) * precision; }

    public static DoubleFunction2Arg swapArgs(final DoubleFunction2Arg function) {
        return new DoubleFunction2Arg() {
            @Override
            public double apply(double a, double b) {
                return function.apply(b, a);
            }
        };
    }
}