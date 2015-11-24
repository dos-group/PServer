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
// TODO: this needs major testing and debugging + cleanup
/**
 * TODO: At the moment remove takes precedent with concurrent operations. Perhaps allow a flag for the user to choose if
 *  add or remove should take precedent
 */
public class LWWSetTestJob extends Program {

    @Unit(at = "0")
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
                LWWSet<Integer> lwws = new LWWSet<>("one", 2, programContext);

                for (int i = 0; i <= 10; i++) {
                    lwws.add(i);
                }

                lwws.finish();

                System.out.println("[DEBUG] Set of node " + programContext.runtimeContext.nodeID + ": "
                        + lwws.getSet());
                System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                        + lwws.getBuffer());
        });
    }

    @Unit(at = "1")
    public void test2(Lifecycle lifecycle) {
        lifecycle.process(() -> {
                LWWSet<Integer> lwws = new LWWSet<>("one", 2, programContext);

                for (int i = 4; i <= 15; i++) {
                    lwws.add(i);
                }

                Thread.sleep(500);

                for (int i = 5; i <= 11; i++) {
                    lwws.remove(i);
                }

                lwws.finish();

                System.out.println("[DEBUG] Set of node " + programContext.runtimeContext.nodeID + ": "
                        + lwws.getSet());
                System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                        + lwws.getBuffer());
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
                .run(LWWSetTestJob.class)
                .done();
    }
}
