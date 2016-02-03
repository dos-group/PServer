package de.tuberlin.pserver.crdt.sets;


import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;


/**
 * A Grow-Only Set supports operations add and lookup. There is no remove operation!
 */

public class TwoPSetTestJob extends Program {
    private static final int NUM_NODES = 2;
    private static final int NUM_REPLICAS = 2;

    private static final String CRDT_ID = "2pSet";

    @Unit
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            TwoPSet<Integer> tpSet = new TwoPSet<>(CRDT_ID, NUM_REPLICAS, programContext);

            if(programContext.nodeID % 2 == 0) {
                for (int i = 0; i <= 10; i++) {
                    tpSet.add(i);
                }
            }
            else {
                for (int i = 4; i <= 15; i++) {
                    tpSet.add(i);
                }
                Thread.sleep(500);

                for (int i = 5; i <= 11; i++) {
                    tpSet.remove(i);
                }

            }

            tpSet.finish();
            result(tpSet.getSet().toArray());

            System.out.println("[DEBUG] Set of node " + programContext.runtimeContext.nodeID + ": " + tpSet.getSet());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + tpSet.getBuffer());
        });
    }

    public static void main(final String[] args) {
        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", String.valueOf(NUM_NODES));
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx256m\"]");

        final List<List<Serializable>> results = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(TwoPSetTestJob.class)
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
