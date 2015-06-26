package de.tuberlin.pserver.ml.playground.optimization2;

public interface Observer<TWeight> {

    public abstract void update(final int epoch, final TWeight weights, final TWeight[] gradientSum);
}
