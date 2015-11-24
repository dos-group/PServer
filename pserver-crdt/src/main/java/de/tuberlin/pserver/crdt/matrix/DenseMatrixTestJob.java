package de.tuberlin.pserver.crdt.matrix;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;

import static org.junit.Assert.assertEquals;

public class DenseMatrixTestJob extends Program {

    @Unit(at = "0")
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            DenseMatrix m = new DenseMatrix(100, 100, "matrix", 2, programContext);
            m.set(10, 10, 33L);

            System.out.println("*** " + m.get(10,10));
            m.finish();

        });
    }

    @Unit(at = "1")
    public void test2(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            DenseMatrix m = new DenseMatrix(100, 100, "matrix", 2, programContext);
            m.finish();
            System.out.println("** " + m.get(10,10));
        });
    }


    public static void main(final String[] args) {

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", "2");
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx256m\"]");

        PServerExecutor.LOCAL
                // Second param is number of slots (threads executing the job) per node,
                // should be 1 at the beginning.
                .run(DenseMatrixTestJob.class)
                .done();
    }
}
