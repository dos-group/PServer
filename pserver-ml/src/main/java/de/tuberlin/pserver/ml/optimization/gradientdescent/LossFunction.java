package de.tuberlin.pserver.ml.optimization.gradientdescent;

public interface LossFunction {

    /**
     * Convex Loss Functions:
     *      + Square loss: L(y, y') = 0.5 * (p - y)²
     *      + Hinge loss: L(y, y') = max{0, 1 − y * y'}
     *      + Exponential loss: L(y, y') = exp(− y * y')
     *      + Logistic loss: L(y, y') = log2(1 + exp(−y * y'))
     *
     * (All of these are convex upper bounds on 0-1 loss)
     */

    /** Evaluate the loss function. */
    public abstract double loss(double p, double y);

    /** Evaluate the derivative of the loss function with respect to the prediction `p`. */
    public abstract double dloss(double p, double y);
}
