package de.tuberlin.pserver.performance;


import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.radt.arrays.Array;
import de.tuberlin.pserver.runtime.core.config.ConfigLoader;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

public class ArrayPerformanceTest extends Program {
    private static final int NUM_NODES = 16;
    private static final int ARRAY_SIZE = 100000;
    private static final int NUM_OPERATIONS = 1000000;
    private static final String RADT_ID = "array";

    @Unit
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {

            Array<Integer> array = new Array<>(ARRAY_SIZE, RADT_ID, NUM_NODES, programContext);
            Random rand = new Random(System.currentTimeMillis());
            List<Pair<Integer, Integer>> buffer = new java.util.LinkedList<>();


            System.out.println("[TEST] " + programContext.nodeID + " Filling operation buffer");

            // 1. Fill the buffer with writes
            for(int i = 0; i < NUM_OPERATIONS; i++) {
                buffer.add(new ImmutablePair<>(rand.nextInt(ARRAY_SIZE), rand.nextInt()));
            }

            UnitMng.barrier(UnitMng.GLOBAL_BARRIER);
            final long startTime = System.currentTimeMillis();

            System.out.println("[TEST] " + programContext.nodeID + " Starting operations");

            for(Pair<Integer, Integer> pair : buffer) {
                array.write(pair.getLeft(), pair.getRight());
            }

            final long intermediateTime = System.currentTimeMillis();

            array.finish();

            final long stopTime = System.currentTimeMillis();

            System.out.println("[TEST] " + programContext.nodeID + " Finished.");

            System.out.println("[TEST] Time: " + (stopTime - startTime) + "ms");

            result(array.getArray(), stopTime - startTime, stopTime - intermediateTime);

        });
    }

    public static void main(final String[] args) {
        final List<List<Serializable>> results = Lists.newArrayList();

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", String.valueOf(NUM_NODES));
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx256m\"]");

        PServerExecutor.DISTRIBUTED
                .run(ConfigLoader.loadResource("distributed.conf"), ArrayPerformanceTest.class)
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
        long convergenceTime = 0;

        for(int i = 0; i < NUM_NODES; i++) {
            System.out.println("[TEST] Node " + i + ": "
                    + "execution time " + results.get(i).get(1) + "ms, "
                    + "convergence time " + results.get(i).get(2) + "ms, ");
            avgTime += (long)results.get(i).get(1);
            convergenceTime += (long)results.get(i).get(2);
        }
        avgTime /= NUM_NODES;
        convergenceTime /= NUM_NODES;

        System.out.println("[TEST] Avg. execution time: " + avgTime + "ms"
                + ", Convergence time: " + convergenceTime);
    }
}
