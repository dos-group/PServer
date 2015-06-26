package de.tuberlin.pserver.ml.playground.optimization2;


public interface PredictionFunction<TFeature, TWeight, TLabel> {

    public abstract TLabel predict(final TFeature features, final TWeight weights);

    public abstract TWeight gradient(final TFeature features, final TWeight weights);
}
