package de.tuberlin.pserver.ml.playground.optimization1;

import de.tuberlin.pserver.commons.UnsafeOp;
import de.tuberlin.pserver.math.DMatrix;
import de.tuberlin.pserver.math.Matrix;

public interface GradientStepFunction {

    public abstract Matrix takeStep(final Matrix weights, final Matrix gradients, final double alpha);

    // ---------------------------------------------------

    class SimpleGradientStep implements GradientStepFunction {

        @Override
        public Matrix takeStep(final Matrix weights, final Matrix gradients, final double alpha) {
            return new DMatrix(weights.rowAsVector().add(-alpha, gradients.rowAsVector()));
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
        public Matrix takeStep(final Matrix weights, final Matrix gradients, final double alpha) {

            for( int i = 0; i < weights.numCols() * weights.numRows(); i++ ) {

                final long value = Double.doubleToRawLongBits(weights.get(0, i) + (-alpha) * gradients.get(0, i));

                UnsafeOp.unsafe.putLongVolatile(weights.toArray(), ((long) i << shift) + base, value);

                //weights.set(i, weights.get(i) + (-alpha) * gradients.get(i));
            }

            return weights;
        }
    }
}
