package de.tuberlin.pserver.performance;


import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.crdt.counters.SimpleCounter;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import org.junit.Test;

import java.io.Serializable;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class CounterPerformanceTest extends Program {
    private static final int NUM_NODES = 4;
    private static final int NUM_OPERATIONS = 10000;

    private static final String CRDT_ID = "counter";

    enum Type {
        INCREMENT,
        DECREMENT
    }

    private class Command {
        Type type;
        int value;

        public Command(Type type, int value) {
            this.type = type;
            this.value = value;
        }
    }

    @Unit
    public void test(final Lifecycle lifecycle) {
        lifecycle.process(() -> {

            SimpleCounter counter = SimpleCounter.newReplica(CRDT_ID, NUM_NODES, programContext);
            Random rand = new Random(Calendar.getInstance().getTimeInMillis());
            List<Command> buffer = new java.util.LinkedList<>();


            System.out.println("[TEST] " + programContext.nodeID + " Filling operation buffer");
            double p = 0;

            // 1. Fill the buffer with writes
            for(int i = 0; i < NUM_OPERATIONS; i++) {
                p = rand.nextDouble();
                if(p < 0.5) {
                    buffer.add(new Command(Type.INCREMENT, 1));
                }
                else {
                    buffer.add(new Command(Type.DECREMENT, 1));
                }
            }

            UnitMng.barrier(UnitMng.GLOBAL_BARRIER);
            final long startTime = Calendar.getInstance().getTimeInMillis();

            System.out.println("[TEST] " + programContext.nodeID + " Starting operations");

            for(Command com : buffer) {
                switch(com.type) {
                    case INCREMENT:
                        counter.increment(com.value);
                        break;
                    case DECREMENT:
                        counter.decrement(com.value);
                        break;
                    default:
                        throw new RuntimeException("Unknown command: " + com.type);
                }
            }

            final long intermediateTime = Calendar.getInstance().getTimeInMillis();

            counter.finish();

            final long stopTime = Calendar.getInstance().getTimeInMillis();

            System.out.println("[TEST] " + programContext.nodeID + " Finished.");

            System.out.println("[TEST] Time: " + (stopTime - startTime) + "ms");

            result(counter.getCount(), stopTime - startTime, stopTime - intermediateTime);

            System.out.println("[DEBUG] Count of node " + programContext.runtimeContext.nodeID + ": " + counter.getCount());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + counter.getBuffer());

        });
    }

    @Test
    public static void main(final String[] args) {
        final List<List<Serializable>> results = Lists.newArrayList();

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", String.valueOf(NUM_NODES));
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx512m\"]");

        PServerExecutor.LOCAL
                .run(CounterPerformanceTest.class)
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
        for(int i = 0; i < NUM_NODES; i++) {
            System.out.println("[TEST] Node " + i + ": "
                    + "execution time " + results.get(i).get(1) + "ms, "
                    + "replication time " + results.get(i).get(2) + "ms, ");
            avgTime += (long)results.get(i).get(1);
        }
        avgTime /= NUM_NODES;

        System.out.println("[TEST] Avg. execution time: " + avgTime + "ms");
    }
}
