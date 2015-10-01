package de.tuberlin.pserver.sets.testjobs;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.registers.LWWRegister;
import de.tuberlin.pserver.registers.RegisterOperation;
import de.tuberlin.pserver.runtime.MLProgram;
import de.tuberlin.pserver.sets.TwoPSet;
import de.tuberlin.pserver.sets.SetOperation;

import java.util.Calendar;

/**
 * A Grow-Only Set supports operations add and lookup. There is no remove operation!
 */

// TODO: this only works to a precision of milliseconds (Date class)!
// Is it ok to use System.nanoTime in multicore systems? http://www.principiaprogramatica.com/?p=16
// TODO: this needs more testing and debugging + cleanup
public class TwoPSetTestJob extends MLProgram {

    @Unit(at = "0")
    public void test(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                TwoPSet<Integer> tps = new TwoPSet<>("one", dataManager);

                for (int i = 0; i <= 10; i++) {
                    tps.add(i, dataManager);
                }

                tps.finish(dataManager);

                System.out.println("[DEBUG] Set of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + tps.getSet());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + tps.getBuffer());
            });
        });
    }

    @Unit(at = "1")
    public void test2(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                TwoPSet<Integer> tps = new TwoPSet<>("one", dataManager);

                for (int i = 4; i <= 15; i++) {
                    tps.add(i, dataManager);
                }
                Thread.sleep(500);

                for (int i = 5; i <= 11; i++) {
                    tps.remove(i, dataManager);
                }

                tps.finish(dataManager);

                System.out.println("[DEBUG] Set of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + tps.getSet());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + tps.getBuffer());
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
                .run(TwoPSetTestJob.class, 1)
                .done();
    }
}
