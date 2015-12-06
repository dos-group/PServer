package de.tuberlin.pserver.matrix;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CRDTDenseMatrix32FTestJob extends Program {
    private final long ROWS = 10;
    private final long COLS = 10;
    private final String NUM_NODES = "2";

    @Unit(at = "0")
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            CRDTDenseMatrix32F m = new CRDTDenseMatrix32F(ROWS, COLS, "one", 2, programContext);

            for (int i = 0; i < ROWS; i++) {
                for(int k = 0; k < COLS; k++) {
                    //System.out.println("[DEBUG:" + programContext.runtimeContext.nodeID + "] Sending: " + i);
                    m.set(i, k, 9.9f);
                }
            }

            m.finish();

            System.out.println("[DEBUG] Node " + programContext.runtimeContext.nodeID + " Matrix 1: ");
            for (int i = 0; i < m.rows(); i++) {
                System.out.println();
                for (int k = 0; k < m.cols(); k++) {
                    System.out.print(m.get(i, k) + " ");
                }
            }
            System.out.println();

            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + m.getBuffer());
            System.out.println("[DEBUG] Queue of node " + programContext.runtimeContext.nodeID + ": " + m.getQueue().size());

        });
    }

    @Unit(at = "1")
    public void test2(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            CRDTDenseMatrix32F m = new CRDTDenseMatrix32F(ROWS, COLS, "one", 2, programContext);

            for(int i = 0; i < ROWS; i++) {
                for(int k = 0; k < COLS; k++) {
                    //System.out.println("[DEBUG:" + programContext.runtimeContext.nodeID + "] Sending: " + i);
                    m.set(i, COLS - k - 1, 1.1f);
                }
            }

            m.set(0, 4, 22f);
            m.set(0, 5, 55f);
//            m.setDiagonalsToZero();

            m.set(5, 5, 100f);
            m.transpose();

            m.finish();

            System.out.println("[DEBUG] Node " + programContext.runtimeContext.nodeID + " Matrix 1: ");
            for(int i = 0; i < m.rows(); i++) {
                System.out.println();
                for(int k = 0; k < m.cols(); k++) {
                    System.out.print(m.get(i, k) + " ");
                }
            }
            System.out.println();


            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + m.getBuffer());
            System.out.println("[DEBUG] Queue of node " + programContext.runtimeContext.nodeID + ": " + m.getQueue().size());

        });
    }

    @Test
    public void main() {

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", NUM_NODES);
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx256m\"]");

        PServerExecutor.LOCAL
                // Second param is number of slots (threads executing the job) per node,
                // should be 1 at the beginning.
                .run(CRDTDenseMatrix32FTestJob.class)
                .done();
    }
}
