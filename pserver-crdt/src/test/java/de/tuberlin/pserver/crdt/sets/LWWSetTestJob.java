package de.tuberlin.pserver.crdt.sets;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.MLProgram;

/**
 * A Grow-Only ISet supports operations add and lookup. There is no remove operation!
 */

// TODO: this only works to a precision of milliseconds (Date class)!
// Is it ok to use System.nanoTime in multicore systems? http://www.principiaprogramatica.com/?p=16
// TODO: this needs major testing and debugging + cleanup
/**
 * TODO: At the moment remove takes precedent with concurrent operations. Perhaps allow a flag for the user to choose if
 *  add or remove should take precedent
 */
public class LWWSetTestJob extends MLProgram {

    @Unit(at = "0")
    public void test(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                LWWSet<Integer> lwws = new LWWSet<>("one", dataManager);

                for (int i = 0; i <= 10; i++) {
                    lwws.add(i);
                }

                lwws.finish();

                System.out.println("[DEBUG] ISet of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + lwws.getSet());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + lwws.getBuffer());
            });
        });
    }

    @Unit(at = "1")
    public void test2(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                LWWSet<Integer> lwws = new LWWSet<>("one", dataManager);

                for (int i = 4; i <= 15; i++) {
                    lwws.add(i);
                }

                Thread.sleep(500);

                for (int i = 5; i <= 11; i++) {
                    lwws.remove(i);
                }

                lwws.finish();

                System.out.println("[DEBUG] ISet of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + lwws.getSet());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + lwws.getBuffer());
            });
        });
    }

    public static void main(final String[] args) {

        // ISet the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", "2");
        // ISet the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx256m\"]");

        PServerExecutor.LOCAL
                // Second param is number of slots (threads executing the job) per node,
                // should be 1 at the beginning.
                .run(LWWSetTestJob.class, 1)
                .done();
    }
}
