package de.tuberlin.pserver.ml.playground.optimization2;


import javafx.util.Pair;

public interface LossFunction<TLabel, TFeature, TWeight> {

    public abstract TLabel loss(final LabeledVector<TLabel, TFeature> v, final TWeight weights);

    public abstract TWeight gradient(final LabeledVector<TLabel, TFeature> v, final TWeight weights);

    public abstract Pair<TLabel, TWeight> lossAndGradient(final LabeledVector<TLabel, TFeature> v, final TWeight weights);
}
