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
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ArrayTestJob extends Program {
    private static final int NUM_NODES = 2;
    private static final int NUM_REPLICAS = 2;
    private static final int ARRAY_SIZE = 5;

    private static final String RADT_ID = "array";

    @Unit
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            Array<Integer> array = new Array<>(ARRAY_SIZE, RADT_ID, NUM_REPLICAS, programContext);

            if(programContext.nodeID % 2 == 0) {
                for (int i = 0; i <= 10; i++) {
                    array.write(1, i);
                }

                Thread.sleep(1000);

                array.write(0, 11);
                array.write(1, 22);
                array.write(2, 33);
                array.write(3, 44);
                array.write(4, 55);
            }
            else {
                for (int i = 20; i <= 30; i++) {
                    array.write(1, i);
                }

                array.write(0, 111);
                array.write(1, 222);
                array.write(2, 333);
                array.write(3, 444);
                array.write(4, 555);
            }
            array.finish();

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
    }
}
