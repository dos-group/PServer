package de.tuberlin.pserver.ml.playground.optimization2;

public interface GradientStepFunction<TWeight> {

    public abstract TWeight takeStep(final TWeight weights, final TWeight gradients, final double alpha);
}
