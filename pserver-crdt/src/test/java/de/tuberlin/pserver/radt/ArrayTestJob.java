package de.tuberlin.pserver.radt;


import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.radt.arrays.Array;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ArrayTestJob extends Program {
    private static final int NUM_NODES = 3;
    private static final int ARRAY_SIZE = 1000;
    private static final int ITERATIONS = 1000;
    private static final int MAX_VALUE = 100000;

    private static final String RADT_ID = "array";

    @Unit
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            Array<Integer> array = new Array<>(ARRAY_SIZE, RADT_ID, NUM_NODES, programContext);
            Random rand = new Random(Calendar.getInstance().getTimeInMillis());

            System.out.println("[TEST] " + programContext.nodeID + " Starting writes");

            for(int i = 0; i < ITERATIONS; i++) {
                array.write(rand.nextInt(ARRAY_SIZE), rand.nextInt(MAX_VALUE));
            }

            array.finish();

            System.out.println("[TEST] " + programContext.nodeID + " Finished.");

            result(array.getArray());

            System.out.println("\n[DEBUG] Array of node " + programContext.runtimeContext.nodeID + ": ");
            System.out.println(array + "\n");
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
                .run(ArrayTestJob.class)
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
