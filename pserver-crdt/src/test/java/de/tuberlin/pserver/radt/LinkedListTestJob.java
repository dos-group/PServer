package de.tuberlin.pserver.radt;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.radt.list.LinkedList;
import org.junit.Test;


public class LinkedListTestJob extends Program {

    @Unit
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            LinkedList<Integer> list = new LinkedList<>("one", 2, programContext);

           /* if(programContext.nodeID % 2 == 0) {
                for (int i = 0; i <= 10; i++) {
                    list.insert(i, i);
                }

            }
            else {
                for (int i = 0; i <= 10; i++) {
                    list.insert(i, i+10);
                }

            }*/

            if(programContext.nodeID % 2 == 0) {
                for (int i = 0; i <= 10; i++) {
                    list.insert(i, i);
                }

                list.insert(0, 33); // insert at head
                list.insert(5, 55); // insert in middle
                list.insert(13, 77); // insert at end
                list.insert(20, 11); // insert outside range (has no effect, should return false)


                list.update(2, 99); // update value


                list.delete(3); // delete an item
                list.insert(3, 1000);
            }
            else {
                for (int i = 0; i <= 10; i++) {
                    list.insert(i, i+10);
                }

                list.insert(0, 333); // insert at head
                list.insert(7, 555); // insert in middle
                list.insert(46, 111); // insert outside range (has no effect, should return false)


                list.update(5, 999); // update value


                list.delete(10); // delete an item

                list.insert(10, 100);
            }

            list.finish();

            System.out.println("[DEBUG] LinkedList of node " + programContext.runtimeContext.nodeID + ": " + list);
        });
    }

    @Test
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
