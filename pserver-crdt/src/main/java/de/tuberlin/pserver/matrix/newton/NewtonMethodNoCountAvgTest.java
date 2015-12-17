package de.tuberlin.pserver.matrix.newton;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.matrix.crdt.NoCountAvgDenseMatrix64F;

import static org.junit.Assert.assertEquals;

public class NewtonMethodNoCountAvgTest extends Program {
    private final long ROWS = 10;
    private final long COLS = 10;
    private static final String NUM_NODES = "2";

    @Unit
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            NoCountAvgDenseMatrix64F m = new NoCountAvgDenseMatrix64F(ROWS, COLS, "one", 2, programContext);

            System.out.println("*");
            new NewtonMethod(m).newton("Newton.csv");
            System.out.println("**");

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
            //System.out.println("[DEBUG] Queue of node " + programContext.runtimeContext.nodeID + ": " + m.getQueue().size());

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
                .run(NewtonMethodNoCountAvgTest.class)
                .done();
    }
}
