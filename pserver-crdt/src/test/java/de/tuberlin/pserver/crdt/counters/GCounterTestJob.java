package de.tuberlin.pserver.crdt.counters;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GCounterTestJob extends Program {

    @Unit(at = "0")
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            GCounter gc = new GCounter("one", 3, programContext);
            GCounter gc2 = new GCounter("two", 2, programContext);

            for (int i = 0; i < 10000; i++) {
                gc.increment(1);
                if ((i % 2) == 0) {
                    gc2.increment(1);
                }
            }

            gc.finish();
            gc2.finish();

            System.out.println("[DEBUG] Count of node " + programContext.runtimeContext.nodeID + ": "
                    + gc.getCount());
            System.out.println("[DEBUG] Count of node " + programContext.runtimeContext.nodeID + ": "
                    + gc2.getCount());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                    + gc.getBuffer());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                    + gc2.getBuffer());
        });
    }

    @Unit(at = "1")
    public void test2(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            GCounter gc = new GCounter("one", 3, programContext);
            GCounter gc2 = new GCounter("two", 2, programContext);

            for (int i = 0; i < 100000; i++) {
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

    @Unit(at = "2")
    public void test3(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            GCounter gc = new GCounter("one", 3, programContext);

            for (int i = 0; i < 10000; i++) {
                gc.increment(1);
            }

            gc.finish();

            System.out.println("[DEBUG] Count of node " + programContext.runtimeContext.nodeID + ": "
                    + gc.getCount());

            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                    + gc.getBuffer());

        });
    }

    private void initializedCounterShouldBeZero() {
        GCounter gc = new GCounter("counter", 2, programContext);
        assertEquals("An initialized GCounter should have count 0", 0L, gc.getCount());
        gc.finish();
    }

    private void incrementShouldIncreaseValue() {
        GCounter gc = new GCounter("counter", 2, programContext);
        gc.increment(10);
        gc.finish();

        assertEquals("Increment should increase the counter", 10, gc.getCount());
    }


    @Test
    public void main() {

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", "3");
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx256m\"]");

        PServerExecutor.LOCAL
                // Second param is number of slots (threads executing the job) per node,
                // should be 1 at the beginning.
                .run(GCounterTestJob.class)
                .done();
    }
}
