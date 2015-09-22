package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.ml.common.LabeledMatrix;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface LossFunction {

    public abstract double loss(final LabeledMatrix v, final Matrix weights);

    public abstract Matrix gradient(final LabeledMatrix v, final Matrix weights);

    public abstract Pair<Double, Matrix> lossAndGradient(final LabeledMatrix v, final Matrix weights);

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
        public double loss(final LabeledMatrix v, final Matrix weights) {
            return lossAndGradient(v, weights).getLeft();
        }

        @Override
        public Matrix gradient(final LabeledMatrix v, final Matrix weights) {
            return lossAndGradient(v, weights).getRight();
        }

        @Override
        public Pair<Double, Matrix> lossAndGradient(LabeledMatrix v, Matrix weights) {

            double prediction = predictionFunction.predict(v.matrix, weights);

            double loss = partialLossFunction.loss(prediction, v.label);

            double lossDerivative = partialLossFunction.derivative(prediction, v.label);

            Matrix gradient = v.matrix.copy(); // predictionFunction.gradient(v.vector, weights)

            gradient.scale(lossDerivative, gradient);

            return Pair.of(loss, gradient);
        }
    }
}
