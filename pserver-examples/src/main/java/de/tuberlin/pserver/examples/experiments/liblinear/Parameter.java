package de.tuberlin.pserver.examples.experiments.liblinear;

import java.io.Serializable;


public class Parameter implements Serializable {

    public SolverType solverType = SolverType.L2_LR;

    public double eps            = 1e-2;

    public double C              = 1.0;

    public int numSlaves         = -1;
}
