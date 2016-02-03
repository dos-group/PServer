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

// TODO: this needs testing and debugging and major cleanup
public class ORSetTestJob extends Program {
    private static final int NUM_NODES = 2;
    private static final int NUM_REPLICAS = 2;

    private static final String CRDT_ID = "orSet";

    @Unit
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            ORSet<Integer> orSet = new ORSet<>(CRDT_ID, NUM_REPLICAS, programContext);

            if(programContext.nodeID % 2 == 0) {
                for (int i = 0; i < 10; i++) {
                    orSet.add(i);
                }
            }
            else {
                for (int i = 10; i <= 15; i++) {
                    orSet.add(i);
                }

                Thread.sleep(500);

                for (int i = 5; i <= 18; i++) {
                    orSet.remove(i);
                }
            }

            orSet.add(99);
            orSet.remove(99);

            orSet.finish();

            result(orSet.getSet().toArray());

            /*
            System.out.println("[DEBUG] Set of node " + programContext.runtimeContext.nodeID + ": " + orSet.getSet());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + orSet.getBuffer());
            */
        });
    }

    public static void main(final String[] args) {
        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", String.valueOf(NUM_NODES));
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx256m\"]");

        final List<java.util.List<Serializable>> results = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(ORSetTestJob.class)
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
