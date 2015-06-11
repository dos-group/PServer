package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.math.DVector;
import de.tuberlin.pserver.math.Vector;
import org.apache.commons.lang3.tuple.Pair;

public interface LossFunction {

    public abstract double loss(final LabeledVector v, final Vector weights);

    public abstract Vector gradient(final LabeledVector v, final Vector weights);

    public abstract Pair<Double, Vector> lossAndGradient(final LabeledVector v, final Vector weights);

    // ---------------------------------------------------

    class GenericLossFunction implements LossFunction {

        public final PredictionFunction predictionFunction;

        public final PartialLossFunction partialLossFunction;

        public GenericLossFunction(final PredictionFunction predictionFunction, final PartialLossFunction lossFunction) {

            this.predictionFunction = predictionFunction;

            this.partialLossFunction = lossFunction;
        }

        @Override
        public double loss(final LabeledVector v, final Vector weights) {
            return lossAndGradient(v, weights).getLeft();
        }

        @Override
        public Vector gradient(final LabeledVector v, final Vector weights) {
            return lossAndGradient(v, weights).getRight();
        }

        @Override
        public Pair<Double, Vector> lossAndGradient(LabeledVector v, Vector weights) {

            double prediction = predictionFunction.predict(v.vector, weights);

            double loss = partialLossFunction.loss(prediction, v.label);

            double lossDerivative = partialLossFunction.derivative(prediction, v.label);

            double[] data = v.vector.toArray();

            Vector weightGradient = new DVector(data.length, data, Vector.VectorType.ROW_VECTOR); // predictionFunction.gradient(v.vector, weights)

            weightGradient.mul(lossDerivative);

            return Pair.of(loss, weightGradient);
        }
    }
}
