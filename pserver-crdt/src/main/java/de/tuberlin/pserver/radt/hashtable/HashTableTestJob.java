package de.tuberlin.pserver.radt.hashtable;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;

/**
 * A Grow-Only Set supports operations add and lookup. There is no remove operation!
 */

// TODO: this only works to a precision of milliseconds (Date class)!
// Is it ok to use System.nanoTime in multicore systems? http://www.principiaprogramatica.com/?p=16
// TODO: this needs more testing and debugging + cleanup
public class HashTableTestJob extends Program {

    @Unit(at = "0")
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            HashTable<Integer, Integer> hashTable = new HashTable<>("one", 2, runtimeManager);

            for (int i = 0; i <= 10; i++) {
               hashTable.put(i, i);
            }

            Thread.sleep(10);

            hashTable.put(0, 11);
            hashTable.put(6, 22);
            hashTable.put(13, 33);
            hashTable.put(7, 44);
            hashTable.put(5, 55);
            hashTable.put(12, 66);

            Thread.sleep(500);

            hashTable.remove(1);
            hashTable.remove(6);

            hashTable.put(1, 99);
            System.out.println("Blub");

            hashTable.finish();
            System.out.println("Blah");

            System.out.println("[DEBUG] HashTable of node " + programContext.runtimeContext.nodeID + ": "
                    + hashTable.toString());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                    + hashTable.getBuffer());
        });
    }

    @Unit(at = "1")
    public void test2(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            HashTable<Integer, Integer> hashTable = new HashTable<>("one", 2, runtimeManager);

            for (int i = 20; i <= 30; i++) {

               hashTable.put(1, i);
            }

            hashTable.put(0, 111);
            hashTable.put(1, 222);
            hashTable.put(2, 333);
            hashTable.put(3, 444);
            hashTable.put(4, 555);

            hashTable.finish();

            System.out.println("[DEBUG] HashTable of node " + programContext.runtimeContext.nodeID + ": "
                    + hashTable.toString());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                    + hashTable.getBuffer());
        });
    }

    public static void main(final String[] args) {

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", "2");
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx256m\"]");

        PServerExecutor.LOCAL
                // Second param is number of slots (threads executing the job) per node,
                // should be 1 at the beginning.
                .run(HashTableTestJob.class)
                .done();
    }
}
