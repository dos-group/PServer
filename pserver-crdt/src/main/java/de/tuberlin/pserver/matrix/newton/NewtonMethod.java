package de.tuberlin.pserver.matrix.newton;


import de.tuberlin.pserver.matrix.crdt.AvgMatrix;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class NewtonMethod {
    private static final double EPSILON = 1E-50;
    private static final double ZERO = -2d;
    private static final double INITIAL = 99999.123456789;


    // Newton: x_1 = x_0 - f(x_0)/f(x_0)
    private AvgMatrix m0;

    public NewtonMethod(AvgMatrix m0) {
        this.m0 = m0;
        for(int i = 0; i < m0.rows(); i++) {
            for(int k = 0; k < m0.cols(); k++) {
                m0.set(i, k, 10000d);
            }
        }
    }

    public AvgMatrix newton(String file) {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter(new File(file)));

            long iterations = 0;
            while(!isConverged()) {
                nextStep();
                write(out);
                iterations++;
            }

            System.out.println(iterations + " iterations.");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(out != null) {
                try {
                    out.flush();
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }



        return m0;
    }

    private synchronized void write(BufferedWriter out) {
        for(int i = 0; i < m0.rows(); i++) {
            for(int k = 0; k < m0.cols(); k++) {
                double x0 = m0.get(i,k);
                try {
                    out.write(String.valueOf(x0));
                    if(i == m0.rows() -1 && k == m0.cols() - 1) out.write("\n");
                    else out.write(",");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private boolean isConverged() {
        for(int i = 0; i < m0.rows(); i++) {
            for(int k = 0; k < m0.cols(); k++) {
                if(m0.get(i,k) < (ZERO - EPSILON) || m0.get(i,k) > (ZERO + EPSILON)) {
                    return false;
                }
            }
        }

        return true;
    }

    private void nextStep() {
        // TODO: make sure I'm not dividing by zero!
        for(int i = 0; i < m0.rows(); i++) {
            for(int k = 0; k < m0.cols(); k++) {
                double x0 = m0.get(i,k);
                if(x0 != ZERO) {
                    System.out.println("Old: " + x0);
                    System.out.println("New: " + (x0 - (Math.pow(x0 +2, 2) / (2 * (x0+2)))));
                    System.out.println();
                    m0.includeInAvg(i, k, x0 - ((Math.pow(x0+2, 2) / (2 * (x0+2)))));
                }
            }
        }
    }
}