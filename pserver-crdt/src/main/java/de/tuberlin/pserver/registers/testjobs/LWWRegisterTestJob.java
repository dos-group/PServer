package de.tuberlin.pserver.registers.testjobs;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.registers.LWWRegister;
import de.tuberlin.pserver.registers.RegisterOperation;
import de.tuberlin.pserver.runtime.MLProgram;

import java.util.Calendar;

// TODO: this only works to a precision of milliseconds (Date class)!
// Is it ok to use System.nanoTime in multicore systems? http://www.principiaprogramatica.com/?p=16
public class LWWRegisterTestJob extends MLProgram {

    @Unit(at = "0")
    public void test(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                LWWRegister<Integer> lwwr = new LWWRegister<>("one", dataManager, (i1, i2) -> i1 > i2);

                for (int i = 0; i <= 10000; i++) {
                    lwwr.applyOperation(new RegisterOperation<>(RegisterOperation.WRITE, i,
                            Calendar.getInstance().getTime()), dataManager);
                }

                lwwr.finish(dataManager);

                System.out.println("[DEBUG] Register of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + lwwr.getValue());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + lwwr.getBuffer());
            });
        });
    }

    @Unit(at = "1")
    public void test2(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                LWWRegister<Integer> lwwr = new LWWRegister<>("one", dataManager, (i1, i2) -> i1 > i2);

                for (int i = 0; i <= 10; i++) {
                    lwwr.applyOperation(new RegisterOperation<>(RegisterOperation.WRITE, i,
                            Calendar.getInstance().getTime()), dataManager);
                    Thread.sleep(500);
                }

                lwwr.finish(dataManager);

                System.out.println("[DEBUG] Register of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + lwwr.getValue());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + lwwr.getBuffer());
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
                .run(LWWRegisterTestJob.class, 1)
                .done();
    }
}
