package de.tuberlin.pserver.performance;


import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.radt.arrays.Array;
import de.tuberlin.pserver.radt.list.LinkedList;
import org.junit.Test;

import java.io.Serializable;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

public class ArrayPerformanceTest extends Program {
    private static final int NUM_NODES = 4;
    private static final int ARRAY_SIZE = 10000;
    private static final int NUM_WRITES = 100000;
    private static final String RADT_ID = "array";

    private class Write {
        int index;
        int value;

        public Write(int index, int value) {
            this.index = index;
            this.value = value;
        }
    }



    @Unit
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {

            Array<Integer> array = new Array<>(ARRAY_SIZE, RADT_ID, NUM_NODES, programContext);
            Random rand = new Random(Calendar.getInstance().getTimeInMillis());
            List<Write> buffer = new java.util.LinkedList<>();


            System.out.println("[TEST] " + programContext.nodeID + " Filling operation buffer");

            // 1. Fill the buffer with writes
            for(int i = 0; i < NUM_WRITES; i++) {
                buffer.add(new Write(rand.nextInt(ARRAY_SIZE), rand.nextInt()));
            }

            UnitMng.barrier(UnitMng.GLOBAL_BARRIER);
            final long startTime = Calendar.getInstance().getTimeInMillis();

            System.out.println("[TEST] " + programContext.nodeID + " Starting operations");

            for(Write w : buffer) {
                array.write(w.index, w.value);
            }

            array.finish();

            final long stopTime = Calendar.getInstance().getTimeInMillis();

            System.out.println("[TEST] " + programContext.nodeID + " Finished.");

            System.out.println("[TEST] Time: " + (stopTime - startTime) + "ms");

            result(array.getArray(), stopTime - startTime);

            //System.out.println("\n[DEBUG] Array of node " + programContext.runtimeContext.nodeID + ": ");
            //System.out.println(array + "\n");
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
                .run(ArrayPerformanceTest.class)
                .results(results)
                .done();

        Object[] firstResult = ((Object[])results.get(0).get(0));

        // Compare results of the CRDTs
        for(int i = 1; i < results.size(); i++) {
            assertArrayEquals("The resulting CRDTs are not identical", firstResult, (Object[])results.get(i).get(0));
        }

        System.out.println("\n[TEST] Passed: CRDTs have converged to consistent state.");

        System.out.println("\n[TEST] ***Results***");
        long avgTime = 0;
        for(int i = 0; i < NUM_NODES; i++) {
            System.out.println("[TEST] Node " + i + ": " + results.get(i).get(1) + "ms, ");
            avgTime += (long)results.get(i).get(1);
        }
        avgTime /= NUM_NODES;

        System.out.println("[TEST] Avg. execution time: " + avgTime + "ms");
    }
}
