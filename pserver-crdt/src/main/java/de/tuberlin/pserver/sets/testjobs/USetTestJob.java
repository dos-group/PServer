package de.tuberlin.pserver.sets.testjobs;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.registers.LWWRegister;
import de.tuberlin.pserver.registers.RegisterOperation;
import de.tuberlin.pserver.runtime.MLProgram;
import de.tuberlin.pserver.sets.USet;
import de.tuberlin.pserver.sets.SetOperation;

import java.util.Calendar;

// TODO: this needs more testing and debugging + cleanup + testing for throwing an exception
public class USetTestJob extends MLProgram {

    @Unit(at = "0")
    public void test(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                USet<Integer> us = new USet<>("one", dataManager);

                for (int i = 0; i <= 10; i++) {
                    us.applyOperation(new SetOperation<>(SetOperation.ADD, i), dataManager);
                }

                us.finish(dataManager);

                System.out.println("[DEBUG] Set of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + us.getValue());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + us.getBuffer());
            });
        });
    }

    @Unit(at = "1")
    public void test2(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                USet<Integer> us = new USet<>("one", dataManager);

                for (int i = 16; i <= 20; i++) {

                    us.applyOperation(new SetOperation<>(SetOperation.ADD, i), dataManager);
                }

                Thread.sleep(500);

                for (int i = 5; i <= 10; i++) {

                    us.applyOperation(new SetOperation<>(SetOperation.REMOVE, i), dataManager);
                }

                // Exception:
                //us.applyOperation(new SetOperation<>(SetOperation.ADD, 1), dataManager);


                us.finish(dataManager);

                System.out.println("[DEBUG] Set of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + us.getValue());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + us.getBuffer());
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
                .run(USetTestJob.class, 1)
                .done();
    }
}
