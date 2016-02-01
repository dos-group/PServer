package de.tuberlin.pserver.crdt.counters;


import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import org.junit.Test;

import java.io.Serializable;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SimpleCounterTestJob extends Program {
    private static final int NUM_NODES = 2;
    private static final int NUM_REPLICAS = 2;

    private static final String CRDT_ID1 = "all";
    private static final String CRDT_ID2 = "odd";

    @Unit
    public void test(final Lifecycle lifecycle) {
        lifecycle.process(() -> {
            SimpleCounter gc = SimpleCounter.newReplica(CRDT_ID1, NUM_REPLICAS, programContext);
            SimpleCounter gc2 = SimpleCounter.newReplica(CRDT_ID2, NUM_REPLICAS, programContext);

            for (int i = 1; i < 10000; i++) {
                gc.decrement(i);

                if ((i % 2) == 0) {
                    gc2.decrement(i);
                }
            }

            gc.finish();
            gc2.finish();

            result(gc.getCount(), gc2.getCount());

            /*
            System.out.println("[DEBUG] Count of node " + programContext.runtimeContext.nodeID + ": " + gc.getCount());
            System.out.println("[DEBUG] Count of node " + programContext.runtimeContext.nodeID + ": " + gc2.getCount());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + gc.getBuffer());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + gc2.getBuffer());
            */
        });
    }

    @Test
    public static void main(final String[] args) {
        final List<List<Serializable>> results = Lists.newArrayList();

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", String.valueOf(NUM_REPLICAS));
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx512m\"]");

        PServerExecutor.LOCAL
                .run(SimpleCounterTestJob.class)
                .results(results)
                .done();

        // Compare results of the CRDTs
        assertEquals("Blub", (Long)results.get(0).get(0), (Long)results.get(1).get(0));
        assertEquals("Blub", (Long)results.get(0).get(1), (Long)results.get(1).get(1));
    }
}
