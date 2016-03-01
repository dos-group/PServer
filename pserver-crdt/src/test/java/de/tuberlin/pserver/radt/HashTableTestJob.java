package de.tuberlin.pserver.radt;


import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.radt.hashtable.HashTable;
import org.jblas.util.Random;
import org.junit.Test;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertArrayEquals;

public class HashTableTestJob extends Program {
    private static final int NUM_NODES = 3;
    private static final int NUM_ELEMENTS = 10;


    private static final String RADT_ID = "hashtable";

    @Unit
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            HashTable<Integer, Integer> hashTable = new HashTable<>(RADT_ID, NUM_NODES, programContext);

            System.out.println("[TEST] " + programContext.nodeID + " Starting puts");

            IntStream.range(0, NUM_ELEMENTS)
                    .forEach(i -> hashTable.put(Random.nextInt(NUM_ELEMENTS), Random.nextInt(NUM_ELEMENTS)));

            System.out.println("[TEST] " + programContext.nodeID + " Starting removes");

            IntStream.range(0,NUM_ELEMENTS/5)
                    .forEach(i -> hashTable.remove(Random.nextInt(NUM_ELEMENTS / 2)));

            IntStream.range(0, NUM_ELEMENTS)
                    .forEach(i -> hashTable.put(Random.nextInt(NUM_ELEMENTS), Random.nextInt(NUM_ELEMENTS)));

            hashTable.finish();


            List<int[]> result = hashTable.getEntrySet().stream()
                    .map(entry -> new int[]{entry.getKey(), entry.getValue()})
                    .collect(Collectors.toList());

            result(result.toArray());

            System.out.println("[TEST] " + programContext.nodeID + " Finished.");

            System.out.println("\n[DEBUG] HashTable of node " + programContext.runtimeContext.nodeID + ": "
                    + hashTable.toString());
            System.out.println("[DEBUG] Queue of HashTable of node " + programContext.runtimeContext.nodeID + ": "
                    + hashTable.getQueue().size());
            System.out.println("[DEBUG] Tombstones of HashTable of node " + programContext.runtimeContext.nodeID + ": "
                    + hashTable.getTombstones());
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
                .run(HashTableTestJob.class)
                .results(results)
                .done();

        // TODO: assert test results.

        Object[] firstResult = ((Object[])results.get(0).get(0));

        //Compare results of the CRDTs
        for(int i = 1; i < results.size(); i++) {
            assertArrayEquals("The resulting CRDTs are not identical", firstResult, (Object[])results.get(i).get(0));
        }

        System.out.println("[TEST] Passed: CRDTs have converged to consistent state.");
    }
}
