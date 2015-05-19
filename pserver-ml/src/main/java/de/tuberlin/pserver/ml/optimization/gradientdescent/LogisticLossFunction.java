package de.tuberlin.pserver.ml.optimization.gradientdescent;

public final class LogisticLossFunction implements LossFunction {

    @Override
    public double loss(final double p, final double y) { throw new UnsupportedOperationException(); }

    //  s(t) = 1 / (1 + sigma), where sigma = exp(-ΘTx)
    @Override
    public double dloss(final double p, final double y) { return  (1 / (1 + Math.exp(-p))) - y; }
}
