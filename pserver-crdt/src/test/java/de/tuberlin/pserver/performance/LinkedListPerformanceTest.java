package de.tuberlin.pserver.performance;


import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.radt.list.LinkedList;
import org.jblas.util.Random;
import org.junit.Test;

import java.io.Serializable;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;


public class LinkedListPerformanceTest extends Program {
    private static final int NUM_NODES = 5;
    private static final String RADT_ID = "linkedList";
    private static final int NUM_ELEMENTS = 1000;


    @Unit
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            LinkedList<Integer> list = new LinkedList<>(RADT_ID, NUM_NODES, programContext);

            System.out.println("[TEST] " + programContext.nodeID + " Starting inserts");

            for (int i = 0; i <= NUM_ELEMENTS; i++) {
                if (list.size() > 10) {
                    list.insert(Random.nextInt(list.size()), i * (programContext.nodeID + 1));
                }
                else {
                    list.insert(i , i * (programContext.nodeID + 1));

                }
            }

            System.out.println("[TEST] " + programContext.nodeID + " Starting updates");
            for (int i = 0; i <= NUM_ELEMENTS/2; i++) {
                list.update(Random.nextInt(NUM_ELEMENTS), i * (programContext.nodeID + 1));
            }

            System.out.println("[TEST] " + programContext.nodeID + " Starting deletes");

            for (int i = 0; i < NUM_ELEMENTS/5; i++) {
                list.delete(Random.nextInt(NUM_ELEMENTS/2));
            }

            Thread.sleep(3000);

            for (int i = 0; i <= 10; i++) {
                list.insert(Random.nextInt(list.size()), Random.nextInt(NUM_ELEMENTS/2));
            }

            System.out.println("[TEST] " + programContext.nodeID + " Finished.");

            list.finish();

            result(list.getList().toArray());

            System.out.println("[DEBUG] LinkedList of node " + programContext.runtimeContext.nodeID + ": " + list.getList());
            System.out.println("[DEBUG] Tombstones in LinkedList of node " + programContext.runtimeContext.nodeID + ": " + list.getTombstones());
            System.out.println("[DEBUG] Queue of LinkedList of node " + programContext.runtimeContext.nodeID + ": " + list.getQueue().size());
            /*for(RADTOperation op : list.getQueue()) {
                StringBuilder sb = new StringBuilder();
                sb.append("[");
                for(int i = 0; i < op.getVectorClock().length; i++) {
                    sb.append(op.getVectorClock()[i] + ", ");
                }
                sb.append("]");
                System.out.println(sb.toString());
            }*/
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
                .run(LinkedListPerformanceTest.class)
                .results(results)
                .done();

        Object[] firstResult = ((Object[])results.get(0).get(0));

        // Compare results of the CRDTs
        for(int i = 1; i < results.size(); i++) {
            assertArrayEquals("The resulting CRDTs are not identical", firstResult, (Object[])results.get(i).get(0));
        }
        System.out.println("[TEST] Passed: CRDTs have converged to consistent state.");

    }
}
