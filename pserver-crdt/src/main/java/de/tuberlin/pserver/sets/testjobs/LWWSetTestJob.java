package de.tuberlin.pserver.sets.testjobs;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.registers.LWWRegister;
import de.tuberlin.pserver.registers.RegisterOperation;
import de.tuberlin.pserver.runtime.MLProgram;
import de.tuberlin.pserver.sets.AbstractLWWSet;
import de.tuberlin.pserver.sets.LWWSet;
import de.tuberlin.pserver.sets.SetOperation;

import java.util.Calendar;

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
public class LWWSetTestJob extends MLProgram {

    @Unit(at = "0")
    public void test(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                LWWSet<Integer> lwws = new LWWSet<>("one", dataManager);

                for (int i = 0; i <= 10; i++) {
                    lwws.applyOperation(new SetOperation<>(SetOperation.ADD,
                            new AbstractLWWSet.Pair<>(i, Calendar.getInstance().getTime())), dataManager);
                }

                lwws.finish(dataManager);

                System.out.println("[DEBUG] Set of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + lwws.getSet());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + lwws.getSet());
            });
        });
    }

    @Unit(at = "1")
    public void test2(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                LWWSet<Integer> lwws = new LWWSet<>("one", dataManager);

                for (int i = 4; i <= 15; i++) {

                    lwws.applyOperation(new SetOperation<>(SetOperation.ADD,
                            new AbstractLWWSet.Pair<>(i, Calendar.getInstance().getTime())), dataManager);
                }
                Thread.sleep(500);

                for (int i = 5; i <= 11; i++) {

                    lwws.applyOperation(new SetOperation<>(SetOperation.REMOVE,
                            new AbstractLWWSet.Pair<>(i, Calendar.getInstance().getTime())), dataManager);
                }

                lwws.finish(dataManager);

                System.out.println("[DEBUG] Set of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + lwws.getValue());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + lwws.getBuffer());
            });
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
                .run(LWWSetTestJob.class, 1)
                .done();
    }
}
