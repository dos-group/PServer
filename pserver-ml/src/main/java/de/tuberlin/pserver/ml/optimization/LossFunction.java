package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix.RowIterator;
import de.tuberlin.pserver.runtime.state.MatrixBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public interface LossFunction {

    public abstract double loss(final Matrix X, final Matrix y, final Matrix W, final double lambda);

    public abstract Matrix gradient(final Matrix X, final Matrix y, final Matrix W,
                                    final double lambda, final boolean newtonMethod);

    public abstract Pair<Double, Matrix> lossAndGradient(final Matrix X, final Matrix y,
                                                         final Matrix W, final double lambda,
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
        public double loss(final Matrix X, final Matrix y, final Matrix W, final double lambda) {

            RowIterator XIterator = X.rowIterator();
            RowIterator yIterator = y.rowIterator();

            double sumLoss = 0.0;

            while (XIterator.hasNext()) {
                XIterator.next();
                yIterator.next();

                sumLoss += partialLossFunction.loss(XIterator.get(), (Double)yIterator.get().get(0),
                        predictionFunction.predict(XIterator.get(), W));
            }

            return sumLoss + regularizationFunction.regularize(W, lambda);
        }

        @Override
        public Matrix gradient(final Matrix X, final Matrix y, final Matrix W,
                               final double lambda, final boolean newtonMethod) {

            RowIterator XIterator = X.rowIterator();
            RowIterator yIterator = y.rowIterator();

            Matrix gradient = new MatrixBuilder().dimension(1, X.cols()).build();

            while (XIterator.hasNext()) {

                Matrix derivative = partialLossFunction.derivative(XIterator.get(),
                        (Double)yIterator.get().get(0), predictionFunction.predict(X, W));

                gradient.add(derivative, gradient);

                XIterator.next();
                yIterator.next();
            }

            if (newtonMethod) {

                XIterator.reset();
                yIterator.reset();

                /*
                if (newtonMethod) {
                    Matrix hessian = partialLossFunction.hessian(X, y, predictionFunction.predict(X, W));
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
        public Pair<Double, Matrix> lossAndGradient(final Matrix X, final Matrix y, final Matrix W,
                                                    final double lambda, final boolean newtonMethod) {

            double loss = loss(X, y, W, lambda);

            Matrix gradient = gradient(X, y, W, lambda, newtonMethod);

            return Pair.of(loss, gradient);
        }
    }
}
