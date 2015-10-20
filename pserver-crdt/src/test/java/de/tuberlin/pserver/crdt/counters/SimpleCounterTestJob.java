package de.tuberlin.pserver.crdt.counters;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;

public class SimpleCounterTestJob extends Program {

    @Unit(at = "0")
    public void test(final Lifecycle lifecycle) {
        lifecycle.process(() -> {
            SimpleCounter gc = new SimpleCounter("one", 2, runtimeManager);
            SimpleCounter gc2 = new SimpleCounter("two", 2, runtimeManager);

            for (int i = 0; i < 10000; i++) {
                gc.decrement(1);

                if ((i % 2) == 0) {
                    gc2.decrement(1);
                }
            }

            gc.finish();
            gc2.finish();


            System.out.println("[DEBUG] Count of node " + programContext.runtimeContext.nodeID + ": " + gc.getCount());
            System.out.println("[DEBUG] Count of node " + programContext.runtimeContext.nodeID + ": " + gc2.getCount());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + gc.getBuffer());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + gc2.getBuffer());
        });
    }

    @Unit(at = "1")
    public void test2(Lifecycle lifecycle) {
        lifecycle.process(() -> {
                SimpleCounter gc = new SimpleCounter("one", 2, runtimeManager);
                SimpleCounter gc2 = new SimpleCounter("two", 2, runtimeManager);

                for (int i = 0; i < 50000; i++) {
                    gc.increment(1);

                    if ((i % 2) == 0) {
                        gc2.increment(1);
                    }
                }

                gc.finish();
                gc2.finish();

                System.out.println("[DEBUG] Count of node " + programContext.runtimeContext.nodeID + ": " + gc.getCount());
                System.out.println("[DEBUG] Count of node " + programContext.runtimeContext.nodeID + ": " + gc2.getCount());
                System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + gc.getBuffer());
                System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + gc2.getBuffer());
            });
    }

    public static void main(final String[] args) {

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", "2");
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx512m\"]");

        PServerExecutor.LOCAL
                // Second param is number of slots (threads executing the job) per node,
                // should be 1 at the beginning.
                .run(SimpleCounterTestJob.class)
                .done();
    }
}
