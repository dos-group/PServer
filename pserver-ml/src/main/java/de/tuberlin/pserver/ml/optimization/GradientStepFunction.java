package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.commons.unsafe.UnsafeOp;
import de.tuberlin.pserver.math.vector.Vector;

public interface GradientStepFunction {

    public abstract Vector takeStep(final Vector weights, final Vector gradients, final double alpha);

    // ---------------------------------------------------

    class SimpleGradientStep implements GradientStepFunction {

        @Override
        public Vector takeStep(final Vector weights, final Vector gradients, final double alpha) {
            return weights.add(-alpha, gradients);
        }
    }

    class AtomicGradientStep implements GradientStepFunction {

        private static final int base = UnsafeOp.unsafe.arrayBaseOffset(long[].class);

        private static final int shift;

        static {
            int scale = UnsafeOp.unsafe.arrayIndexScale(long[].class);

            if ((scale & (scale - 1)) != 0)
                throw new Error();

            shift = 31 - Integer.numberOfLeadingZeros(scale);
        }

        @Override
        public Vector takeStep(final Vector weights, final Vector gradients, final double alpha) {
            for( int i = 0; i < weights.length(); i++ ) {

                final long value = Double.doubleToRawLongBits(weights.get(i) + (-alpha) * gradients.get(i));

                UnsafeOp.unsafe.putLongVolatile(weights.toArray(), ((long) i << shift) + base, value);
            }
            return weights;
        }
    }
}
