package de.tuberlin.pserver.performance;


import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.crdt.counters.SimpleCounter;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.runtime.core.config.ConfigLoader;

import java.io.Serializable;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CounterPerformanceTest extends Program {
    private static final int NUM_NODES = 4;
    private static final int NUM_OPERATIONS = 1000000;

    private static final String CRDT_ID = "counter";

    @Unit
    public void test(final Lifecycle lifecycle) {
        lifecycle.process(() -> {

            SimpleCounter counter = SimpleCounter.newReplica(CRDT_ID, NUM_NODES, programContext);

            UnitMng.barrier(UnitMng.GLOBAL_BARRIER);

            final long startTime = System.currentTimeMillis();

            System.out.println("[TEST] " + programContext.nodeID + " Starting operations");

            for(int i = 0; i < NUM_OPERATIONS; i++) {
                counter.increment(1);
            }

            final long intermediateTime = System.currentTimeMillis();

            counter.finish();

            final long stopTime = System.currentTimeMillis();

            System.out.println("[TEST] " + programContext.nodeID + " Finished.");

            System.out.println("[TEST] Time: " + (stopTime - startTime) + "ms");

            result(counter.getCount(), stopTime - startTime, stopTime - intermediateTime);

            System.out.println("[DEBUG] Count of node " + programContext.runtimeContext.nodeID + ": " + counter.getCount());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + counter.getBuffer());

        });
    }

    public static void main(final String[] args) {
        final List<List<Serializable>> results = Lists.newArrayList();

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        //System.setProperty("simulation.numNodes", String.valueOf(NUM_NODES));
        // Set the memory each simulated node gets.
        //System.setProperty("jvmOptions", "[\"-Xmx512m\"]");

        PServerExecutor.DISTRIBUTED
                .run(ConfigLoader.loadResource("distributed.conf"), CounterPerformanceTest.class)
                .results(results)
                .done();

        // Compare results of the CRDTs
        long firstCount = (Long)results.get(0).get(0);

        for(int i = 1; i < NUM_NODES; i++) {
            assertEquals("CRDTs not equal", firstCount, (long)results.get(1).get(0));
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
