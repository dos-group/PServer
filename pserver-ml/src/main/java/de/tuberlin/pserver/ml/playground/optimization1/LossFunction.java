package de.tuberlin.pserver.ml.playground.optimization1;

import de.tuberlin.pserver.math.matrix.dense.DMatrix;
import de.tuberlin.pserver.math.vector.dense.DVector;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.ml.common.LabeledVector;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface LossFunction {

    public abstract double loss(final LabeledVector v, final Matrix weights);

    public abstract Matrix gradient(final LabeledVector v, final Matrix weights);

    public abstract Pair<Double, Matrix> lossAndGradient(final LabeledVector v, final Matrix weights);

    // ---------------------------------------------------

    class GenericLossFunction implements LossFunction {

        private static final Logger LOG = LoggerFactory.getLogger(GenericLossFunction.class);

        public final PredictionFunction predictionFunction;

        public final PartialLossFunction partialLossFunction;

        public GenericLossFunction(final PredictionFunction predictionFunction, final PartialLossFunction lossFunction) {

            this.predictionFunction = predictionFunction;

            this.partialLossFunction = lossFunction;
        }

        @Override
        public double loss(final LabeledVector v, final Matrix weights) {
            return lossAndGradient(v, weights).getLeft();
        }

        @Override
        public Matrix gradient(final LabeledVector v, final Matrix weights) {
            return lossAndGradient(v, weights).getRight();
        }

        @Override
        public Pair<Double, Matrix> lossAndGradient(LabeledVector v, Matrix weights) {

            double prediction = predictionFunction.predict(v.vector, weights);

            double loss = partialLossFunction.loss(prediction, v.label);

            double lossDerivative = partialLossFunction.derivative(prediction, v.label);

            Vector weightGradient = new DVector((DVector)v.vector, Vector.Layout.COLUMN_LAYOUT); // predictionFunction.gradient(v.vector, weights)

            weightGradient.mul(lossDerivative);

            return Pair.of(loss, new DMatrix(weightGradient));
        }
    }
}
