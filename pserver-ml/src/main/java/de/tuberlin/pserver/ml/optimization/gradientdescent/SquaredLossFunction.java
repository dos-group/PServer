package de.tuberlin.pserver.ml.optimization.gradientdescent;


public class SquaredLossFunction implements LossFunction {

    @Override
    public double loss(double p, double y) { return 0.5 * (p - y) * (p - y); }

    @Override
    public double dloss(double p, double y) { return (p - y); }
}
