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
public class TwoPSetTestJob extends Program {

    @Unit(at = "0")
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            TwoPSet<Integer> tps = new TwoPSet<>("one", 2, programContext);

            for (int i = 0; i <= 10; i++) {
                tps.add(i);
            }

            tps.finish();

            System.out.println("[DEBUG] Set of node " + programContext.runtimeContext.nodeID + ": " + tps.getSet());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + tps.getBuffer());
        });
    }

    @Unit(at = "1")
    public void test2(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            TwoPSet<Integer> tps = new TwoPSet<>("one", 2, programContext);

            for (int i = 4; i <= 15; i++) {
                tps.add(i);
            }
            Thread.sleep(500);

            for (int i = 5; i <= 11; i++) {
                tps.remove(i);
            }

            tps.finish();

            System.out.println("[DEBUG] Set of node " + programContext.runtimeContext.nodeID + ": " + tps.getSet());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + tps.getBuffer());
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
                .run(TwoPSetTestJob.class)
                .done();
    }
}
