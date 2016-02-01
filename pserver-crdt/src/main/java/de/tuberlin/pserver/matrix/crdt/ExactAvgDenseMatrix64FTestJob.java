package de.tuberlin.pserver.matrix.crdt;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix64F;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExactAvgDenseMatrix64FTestJob extends Program {
    private static final long ROWS = 10;
    private static final long COLS = 10;
    private static final String NUM_NODES = "2";

    @Unit(at = "0")
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            ExactAvgDenseMatrix64F m = new ExactAvgDenseMatrix64F(ROWS, COLS, "one", 2, programContext);

            for (int i = 0; i < ROWS; i++) {
                for(int k = 0; k < COLS; k++) {
                    //System.out.println("[DEBUG:" + programContext.runtimeContext.nodeID + "] Sending: " + i);
                    m.includeInAvg(i, k, 10d);
                }
            }

            System.out.println("[DEBUG] Node " + programContext.runtimeContext.nodeID + " Intermediate Matrix 1: ");
            for(int i = 0; i < m.rows(); i++) {
                System.out.println();
                for(int k = 0; k < m.cols(); k++) {
                    System.out.print(m.get(i, k) + " ");
                }
            }
            System.out.println();

            for (int i = 0; i < ROWS; i++) {
                for(int k = 0; k < COLS; k++) {
                    //System.out.println("[DEBUG:" + programContext.runtimeContext.nodeID + "] Sending: " + i);
                    m.includeInAvg(i, k, 30d);
                }
            }

            m.finish();

            System.out.println("[DEBUG] Node " + programContext.runtimeContext.nodeID + " Final Matrix 1: ");
            for (int i = 0; i < m.rows(); i++) {
                System.out.println();
                for (int k = 0; k < m.cols(); k++) {
                    System.out.print(m.get(i, k) + " ");
                }
            }
            System.out.println();

            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + m.getBuffer());
            //System.out.println("[DEBUG] Queue of node " + programContext.runtimeContext.nodeID + ": " + m.getQueue().size());

        });
    }

    @Unit(at = "1")
    public void test2(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            ExactAvgDenseMatrix64F m = new ExactAvgDenseMatrix64F(ROWS, COLS, "one", 2, programContext);

            for(int i = 0; i < ROWS; i++) {
                for(int k = 0; k < COLS; k++) {
                    //System.out.println("[DEBUG:" + programContext.runtimeContext.nodeID + "] Sending: " + i);
                    m.includeInAvg(i, COLS - k - 1, 1.1d);
                }
            }

            System.out.println("[DEBUG] Node " + programContext.runtimeContext.nodeID + " Intermediate Matrix 1: ");
            for(int i = 0; i < m.rows(); i++) {
                System.out.println();
                for(int k = 0; k < m.cols(); k++) {
                    System.out.print(m.get(i, k) + " ");
                }
            }
            System.out.println();


            for(int i = 0; i < ROWS; i++) {
                for(int k = 0; k < COLS; k++) {
                    //System.out.println("[DEBUG:" + programContext.runtimeContext.nodeID + "] Sending: " + i);
                    m.includeInAvg(i, COLS - k - 1, 20d);
                }
            }

            DenseMatrix64F b = new DenseMatrix64F(ROWS, COLS);
            for(int i = 0; i < ROWS; i++) {
                for(int k = 0; k < COLS; k++) {
                    //System.out.println("[DEBUG:" + programContext.runtimeContext.nodeID + "] Sending: " + i);
                    b.set(i, k, 10d);
                }
            }

            m.sub(b);

            m.finish();

            System.out.println("[DEBUG] Node " + programContext.runtimeContext.nodeID + " Final Matrix 1: ");
            for(int i = 0; i < m.rows(); i++) {
                System.out.println();
                for(int k = 0; k < m.cols(); k++) {
                    System.out.print(m.get(i, k) + " ");
                }
            }
            System.out.println();


            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + m.getBuffer());
           // System.out.println("[DEBUG] Queue of node " + programContext.runtimeContext.nodeID + ": " + m.getQueue().size());

        });
    }

    public static void main(String[] args) {

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", NUM_NODES);
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx256m\"]");

        PServerExecutor.LOCAL
                // Second param is number of slots (threads executing the job) per node,
                // should be 1 at the beginning.
                .run(ExactAvgDenseMatrix64FTestJob.class)
                .done();
    }
}
