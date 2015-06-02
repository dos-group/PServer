package de.tuberlin.pserver.ml.playground.mahout;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implements the Gaussian prior.  This prior has a tendency to decrease large coefficients toward zero, but
 * doesn't tend to set them to exactly zero.
 */
public class L2 implements PriorFunction {

    private static final double HALF_LOG_2PI = Math.log(2.0 * Math.PI) / 2.0;

    private double s2;
    private double s;

    public L2(double scale) {
        s = scale;
        s2 = scale * scale;
    }

    public L2() {
        s = 1.0;
        s2 = 1.0;
    }

    @Override
    public double age(double oldValue, double generations, double learningRate) {
        return oldValue * Math.pow(1.0 - learningRate / s2, generations);
    }

    @Override
    public double logP(double betaIJ) {
        return -betaIJ * betaIJ / s2 / 2.0 - Math.log(s) - HALF_LOG_2PI;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(s2);
        out.writeDouble(s);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        s2 = in.readDouble();
        s = in.readDouble();
    }
}