package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.runtime.state.MatrixBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public interface LossFunction {

    public abstract float loss(final Matrix32F X, final Matrix32F y, final Matrix32F W, final float lambda);

    public abstract Matrix32F gradient(final Matrix32F X, final Matrix32F y, final Matrix32F W,
                                    final float lambda, final boolean newtonMethod);

    public abstract Pair<Float, Matrix32F> lossAndGradient(final Matrix32F X, final Matrix32F y,
                                                         final Matrix32F W, final float lambda,
                                                         final boolean newtonMethod);

    // ---------------------------------------------------

    class GenericLossFunction implements LossFunction {

        private static final Logger LOG = LoggerFactory.getLogger(GenericLossFunction.class);

        public final RegularizationFunction regularizationFunction;

        public final PredictionFunction predictionFunction;

        public final PartialLossFunction partialLossFunction;


        public GenericLossFunction(final PredictionFunction predictionFunction,
                                   final PartialLossFunction lossFunction,
                                   final RegularizationFunction regularizationFunction) {

            this.predictionFunction = predictionFunction;

            this.partialLossFunction = lossFunction;

            this.regularizationFunction = regularizationFunction;
        }

        @Override
        public float loss(final Matrix32F X, final Matrix32F y, final Matrix32F W, final float lambda) {

            Matrix32F.RowIterator XIterator = X.rowIterator();
            Matrix32F.RowIterator yIterator = y.rowIterator();

            float sumLoss = 0.0f;

            while (XIterator.hasNext()) {
                XIterator.next();
                yIterator.next();

                sumLoss += partialLossFunction.loss(XIterator.get(), (Float)yIterator.get().get(0),
                        predictionFunction.predict(XIterator.get(), W));
            }

            return sumLoss + regularizationFunction.regularize(W, lambda);
        }

        Matrix32F gradient = null;

        @Override
        public Matrix32F gradient(final Matrix32F X, final Matrix32F y, final Matrix32F W,
                               final float lambda, final boolean newtonMethod) {

            Matrix32F.RowIterator XIterator = X.rowIterator();
            Matrix32F.RowIterator yIterator = y.rowIterator();


            if (gradient == null) {
                gradient = new MatrixBuilder().dimension(1, X.cols()).build();
            } else {
                gradient.assign(0f);
            }

            //Matrix32F gradient = new MatrixBuilder().dimension(1, X.cols()).build();

            while (XIterator.hasNext()) {

                Matrix32F derivative = partialLossFunction.derivative(XIterator.get(),
                        (float)yIterator.get().get(0), predictionFunction.predict(X, W));

                gradient.add(derivative, gradient);

                XIterator.next();
                yIterator.next();
            }

            if (newtonMethod) {

                XIterator.reset();
                yIterator.reset();

                /*
                if (newtonMethod) {
                    Matrix32F hessian = partialLossFunction.hessian(X, y, predictionFunction.predict(X, W));
                    double hessianRegularization = regularizationFunction.regularizeHessian(W, lambda);

                    return (hessian.applyOnElements(e -> e + hessianRegularizer)).invert().dot(derivative.add(regularizer));
                } else {
                    return derivative.add(regularizer);
                }
                */
            }

            gradient.add(regularizationFunction.regularizeDerivative(W, lambda), gradient);

            return gradient;
        }

        @Override
        public Pair<Float, Matrix32F> lossAndGradient(final Matrix32F X, final Matrix32F y, final Matrix32F W,
                                                    final float lambda, final boolean newtonMethod) {

            float loss = loss(X, y, W, lambda);

            Matrix32F gradient = gradient(X, y, W, lambda, newtonMethod);

            return Pair.of(loss, gradient);
        }
    }
}
