package de.tuberlin.pserver.radt.list;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;

/**
 * A Grow-Only Set supports operations add and lookup. There is no remove operation!
 */

// TODO: this needs more testing and debugging + cleanup
public class LinkedListTestJob extends Program {

    @Unit(at = "0")
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            LinkedList<Integer> list = new LinkedList<>("one", 2, programContext);

            for (int i = 0; i <= 10; i++) {
                list.insert(i, i);
            }

            Thread.sleep(10);

            list.insert(0, 99); // TODO: insert at head gives error if head is not already null!
            list.insert(1, 11);
            list.insert(6, 22);
            //list.insert(13, 33);
            list.insert(7, 44);
            list.insert(5, 55);
            //list.insert(12, 66);
            list.insert(2, 1000);

            Thread.sleep(500);

            list.update(5, 12345);

            System.out.println(list);

            list.delete(6);

            System.out.println(list);

            list.finish();

            System.out.println("[DEBUG] LinkedList of node " + programContext.runtimeContext.nodeID + ": "
                    + list.toString());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                    + list.getBuffer());
        });
    }

    @Unit(at = "1")
    public void test2(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            LinkedList<Integer> list = new LinkedList<>("one", 2, programContext);
            list.finish();

            System.out.println("[DEBUG] LinkedList of node " + programContext.runtimeContext.nodeID + ": "
                    + list.toString());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                    + list.getBuffer());
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
                .run(LinkedListTestJob.class)
                .done();
    }
}
