package de.tuberlin.pserver.radt.arrays;


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
public class ArrayTestJob extends Program {

    @Unit(at = "0")
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            Array<Integer> array = new Array<>(5, "one", 2, runtimeManager);

            for (int i = 0; i <= 10; i++) {
                array.write(1, i);
            }

            Thread.sleep(10);

            array.write(0, 11);
            array.write(1, 22);
            array.write(2, 33);
            array.write(3, 44);
            array.write(4, 55);

            array.finish();

            Object[] result = array.getArray();
            System.out.println("[DEBUG] Array of node " + programContext.runtimeContext.nodeID + ": ");
            for(Object i : result) {
                System.out.println("   " + i);
            }
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                    + array.getBuffer());
            System.out.println("[DEBUG] Queue of node " + programContext.runtimeContext.nodeID + ": "
                    + array.getQueue());
        });
    }

    @Unit(at = "1")
    public void test2(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            Array<Integer> array = new Array<>(5, "one", 2, runtimeManager);

            for (int i = 20; i <= 30; i++) {

                array.write(1, i);
            }

            array.write(0, 111);
            array.write(1, 222);
            array.write(2, 333);
            array.write(3, 444);
            array.write(4, 555);

            array.finish();

            Object[] result = array.getArray();
            System.out.println("[DEBUG] Array of node " + programContext.runtimeContext.nodeID + ": ");
            for(Object i : result) {
                System.out.println("   " + i);
            }
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                    + array.getBuffer());
            System.out.println("[DEBUG] Queue of node " + programContext.runtimeContext.nodeID + ": "
                    + array.getQueue().size());
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
                .run(ArrayTestJob.class)
                .done();
    }
}
