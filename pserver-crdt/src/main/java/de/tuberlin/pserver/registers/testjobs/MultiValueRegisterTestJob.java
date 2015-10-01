package de.tuberlin.pserver.registers.testjobs;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.registers.MultiValueRegister;
import de.tuberlin.pserver.registers.RegisterOperation;
import de.tuberlin.pserver.runtime.MLProgram;

import java.util.Calendar;

// TODO: this only works to a precision of milliseconds (Date class)!
// Is it ok to use System.nanoTime in multicore systems? http://www.principiaprogramatica.com/?p=16
// TODO: This needs more testing and validation
// TODO: if datamanager was serializable, I wouldn't need to pass it to all these damn functions...
// TODO: better soution for the getRegister method to return a set
public class MultiValueRegisterTestJob extends MLProgram {

    @Unit(at = "0")
    public void test(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                MultiValueRegister<Integer> mvr = new MultiValueRegister<>("one", dataManager);

                for (int i = 0; i <= 10000; i++) {
                    mvr.set(i, dataManager);
                }

                mvr.finish(dataManager);

                System.out.println("[DEBUG] Register of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + mvr.getRegister());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + mvr.getBuffer());
            });
        });
    }

    @Unit(at = "1")
    public void test2(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                MultiValueRegister<Integer> mvr = new MultiValueRegister<>("one", dataManager);

                for (int i = 0; i <= 1000; i++) {
                    mvr.set(i, dataManager);
                }

                mvr.finish(dataManager);

                System.out.println("[DEBUG] Register of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + mvr.getRegister());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + mvr.getBuffer());
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
                .run(MultiValueRegisterTestJob.class, 1)
                .done();
    }
}
