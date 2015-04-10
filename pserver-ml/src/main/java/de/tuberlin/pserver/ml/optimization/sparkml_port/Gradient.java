package de.tuberlin.pserver.ml.optimization.sparkml_port;

import org.apache.commons.lang3.tuple.Pair;
import org.jblas.DoubleMatrix;
import org.jblas.SimpleBlas;

import java.io.Serializable;


public abstract class Gradient implements Serializable {
    /**
     * Compute the gradient and loss given the features of a single data point.
     *
     * @param data features for one data point
     * @param label label for this data point
     * @param weights weights/coefficients corresponding to features
     *
     * @return (gradient: Vector, loss: Double)
     */
    public abstract Pair<DoubleMatrix,Double> compute(
            final DoubleMatrix data,
            final double label,
            final DoubleMatrix weights
    );

    /**
     * Compute the gradient and loss given the features of a single data point,
     * add the gradient to a provided vector to avoid creating new objects, and return loss.
     *
     * @param data features for one data point
     * @param label label for this data point
     * @param weights weights/coefficients corresponding to features
     * @param cumGradient the computed gradient will be added to this vector
     *
     * @return loss
     */
    public abstract double compute(
            final DoubleMatrix data,
            final double label,
            final DoubleMatrix weights,
            final DoubleMatrix cumGradient
    );

    /**
     * Compute gradient and loss for a Least-squared loss function, as used in linear regression.
     * This is correct for the averaged least squares loss function (mean squared error)
     *              L = 1/2n ||A weights-y||^2
     * See also the documentation for the precise formulation.
     */
    public static class LeastSquaresGradient extends Gradient {

        public Pair<DoubleMatrix,Double> compute(
                final DoubleMatrix data,
                final double label,
                final DoubleMatrix weights) {

            double diff = SimpleBlas.dot(data, weights) - label;
            double loss = diff * diff / 2.0;
            DoubleMatrix gradient = new DoubleMatrix(data.data);
            gradient.mul(diff);
            return Pair.of(gradient, loss);
        }

        @Override
        public double compute(final DoubleMatrix data,
                              final double label,
                              final DoubleMatrix weights,
                              final DoubleMatrix cumGradient) {
            double diff = SimpleBlas.dot(data, weights) - label;
            SimpleBlas.axpy(diff, data, cumGradient);
            return diff * diff / 2.0;
        }
    }
}
