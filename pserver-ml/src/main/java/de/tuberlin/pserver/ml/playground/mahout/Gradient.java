package de.tuberlin.pserver.ml.playground.mahout;

import de.tuberlin.pserver.math.Vector;

/**
 * Provides the ability to inject a gradient into the SGD logistic regresion.
 * Typical uses of this are to use a ranking score such as AUC instead of a
 * normal loss function.
 */
public interface Gradient {
    Vector apply(String groupKey, int actual, Vector instance, AbstractVectorClassifier classifier);
}