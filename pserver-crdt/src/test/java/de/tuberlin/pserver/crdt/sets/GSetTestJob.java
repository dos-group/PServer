package de.tuberlin.pserver.crdt.sets;


import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * A Grow-Only Set supports operations add and lookup. There is no remove operation!
 */
public class GSetTestJob extends Program {
    private static final int NUM_NODES = 2;
    private static final int NUM_REPLICAS = 2;
    private static final String CRDT_ID = "gSet";

    @Unit
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            GSet<Integer> set = new GSet<>(CRDT_ID, NUM_REPLICAS, programContext);

            for (int i = 1; i <= 11; i++) {
                set.add(i * (programContext.nodeID + 1));
            }

            set.finish();

            result(set.getSet().toArray());

            System.out.println("[DEBUG] Set of node " + programContext.runtimeContext.nodeID + ": " + set.getSet());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + set.getBuffer());
        });
    }

    @Test
    public static void main(final String[] args) {
        final List<List<Serializable>> results = Lists.newArrayList();

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", String.valueOf(NUM_NODES));
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx256m\"]");

        PServerExecutor.LOCAL
                .run(GSetTestJob.class)
                .results(results)
                .done();

        // Compare results of the CRDTs
        Object[] results1 = ((Object[])results.get(0).get(0));
        Object[] results2 = ((Object[])results.get(1).get(0));

        Arrays.sort(results1);
        Arrays.sort(results2);


        assertEquals("The resulting CRDTs are not identical", results1.length, results2.length);

        for(int i = 0; i < results1.length; i++) {
            assertEquals("The resulting CRDTs are not identical", results1[i],results2[i]);
        }
    }
}
