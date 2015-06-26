package de.tuberlin.pserver.ml.playground.optimization2;


public interface PartialLossFunction<TLabel> {

    public abstract TLabel loss(final TLabel prediction, final TLabel label);

    public abstract TLabel derivative(final TLabel prediction, final TLabel label);
}
