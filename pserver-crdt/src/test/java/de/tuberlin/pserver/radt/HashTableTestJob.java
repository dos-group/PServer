package de.tuberlin.pserver.radt;


import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.radt.hashtable.HashTable;

import java.io.Serializable;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class HashTableTestJob extends Program {
    private static final int NUM_NODES = 2;
    private static final int NUM_REPLICAS = 2;

    private static final String RADT_ID = "hashtable";

    @Unit
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            HashTable<Integer, Integer> hashTable = new HashTable<>(RADT_ID, NUM_REPLICAS, programContext);

            if(programContext.nodeID % 2 == 0) {
                hashTable.put(555, 10000);
                hashTable.put(444, 10000);
                hashTable.remove(555);
                hashTable.remove(444);
                Thread.sleep(5000);

                for (int i = 0; i <= 10; i++) {
                    hashTable.put(i, i);
                }
            }
            else {
                for (int i = 20; i <= 30; i++) {

                    hashTable.put(1, i);
                }

                hashTable.put(0, 111);
                hashTable.put(1, 222);
                hashTable.put(2, 333);
                hashTable.put(3, 444);
                hashTable.put(4, 555);
                Thread.sleep(5000);

                hashTable.put(454, 3434);
                hashTable.remove(454);
            }

            hashTable.finish();
            //result(hashTable.get);

            System.out.println("[DEBUG] HashTable of node " + programContext.runtimeContext.nodeID + ": "
                    + hashTable.toString());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                    + hashTable.getBuffer());
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
                .run(HashTableTestJob.class)
                .results(results)
                .done();

        /*Object[] firstResult = ((Object[])results.get(0).get(0));

        // Compare results of the CRDTs
        for(int i = 1; i < results.size(); i++) {
            assertArrayEquals("The resulting CRDTs are not identical", firstResult, (Object[])results.get(i).get(0));
        }*/
    }
}
