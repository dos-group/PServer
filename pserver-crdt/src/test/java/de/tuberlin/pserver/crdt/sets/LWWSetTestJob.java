package de.tuberlin.pserver.crdt.sets;


import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * A last writer wins set.
 */

public class LWWSetTestJob extends Program {
    private static final int NUM_NODES = 2;
    private static final int NUM_REPLICAS = 2;

    private static final String CRDT_ID = "lww";

    @Unit
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            LWWSet<Integer> lwws = new LWWSet<>(CRDT_ID, NUM_REPLICAS, programContext);
            Random rand = new Random(Calendar.getInstance().getTimeInMillis());
            if(programContext.nodeID % 2 != 0) Thread.sleep(2000);

            for (int i = 0; i <= 100; i++) {
                if(programContext.nodeID % 2 != 0) {
                    if(rand.nextFloat() > 0.3) {
                        lwws.remove(i);
                    }
                }
                else {
                    lwws.add(i);
                }
            }

            lwws.finish();
            result(lwws.getSet().toArray());


            System.out.println("[DEBUG] Set of node " + programContext.runtimeContext.nodeID + ": " + lwws.getSet());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + lwws.getBuffer());

        });
    }

    public static void main(final String[] args) {
        final List<List<Serializable>> results = Lists.newArrayList();

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", String.valueOf(NUM_NODES));
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx256m\"]");

        PServerExecutor.LOCAL
                .run(LWWSetTestJob.class)
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
