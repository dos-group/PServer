package de.tuberlin.pserver.crdt.sets;


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
public class GSetTestJob extends Program {

    @Unit(at = "0")
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
                GSet<Integer> gSet = new GSet<>("one", runtimeManager);

                for (int i = 0; i <= 10; i++) {
                    //gSet.applyOperation(new SetOperation<>(Operation.ADD, i), runtimeManager);
                    gSet.add(i);
                }

                gSet.finish();

                System.out.println("[DEBUG] Set of node " + programContext.runtimeContext.nodeID + ": "
                        + gSet.getSet());
                System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                        + gSet.getBuffer());
        });
    }

    @Unit(at = "1")
    public void test2(Lifecycle lifecycle) {
        lifecycle.process(() -> {
                GSet<Integer> gSet = new GSet<>("one", runtimeManager);

                for (int i = 20; i <= 30; i++) {

                    //gSet.applyOperation(new SetOperation<>(Operation.ADD, i), runtimeManager);
                    gSet.add(i);
                }

                gSet.finish();

                System.out.println("[DEBUG] Set of node " + programContext.runtimeContext.nodeID + ": "
                        + gSet.getSet());
                System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                        + gSet.getBuffer());
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
                .run(GSetTestJob.class)
                .done();
    }
}
