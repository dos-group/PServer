package de.tuberlin.pserver.ml.playground.mahout;

import de.tuberlin.pserver.math.Functions;
import de.tuberlin.pserver.math.Vector;

/**
 * Implements the basic logistic training law.
 */
public class DefaultGradient implements Gradient {
    /**
     * Provides a default gradient computation useful for logistic regression.
     *
     * @param groupKey     A grouping key to allow per-something AUC loss to be used for training.
     * @param actual       The target variable value.
     * @param instance     The current feature vector to use for gradient computation
     * @param classifier   The classifier that can compute scores
     * @return  The gradient to be applied to beta
     */
    @Override
    public final Vector apply(String groupKey, int actual, Vector instance, AbstractVectorClassifier classifier) {
        // what does the current model say?
        Vector v = classifier.classify(instance);

        Vector r = v.like();
        if (actual != 0) {
            r.set(actual - 1, 1);
        }
        r.assign(v, Functions.MINUS);
        return r;
    }
}