package de.tuberlin.pserver.sets.testjobs;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.registers.LWWRegister;
import de.tuberlin.pserver.registers.RegisterOperation;
import de.tuberlin.pserver.runtime.MLProgram;
import de.tuberlin.pserver.sets.AbstractLWWSet;
import de.tuberlin.pserver.sets.ORSet;
import de.tuberlin.pserver.sets.SetOperation;

import java.util.Calendar;
import java.util.UUID;

// TODO: this needs testing and debugging and major cleanup
public class ORSetTestJob extends MLProgram {

    @Unit(at = "0")
    public void test(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                ORSet<Integer> ors = new ORSet<>("one", dataManager);

                for (int i = 0; i < 10; i++) {
                    ors.applyOperation(new SetOperation<>(SetOperation.ADD, i, UUID.randomUUID()), dataManager);
                }

                ors.applyOperation(new SetOperation<>(SetOperation.ADD, 7, UUID.randomUUID()), dataManager);

                ors.finish(dataManager);

                System.out.println("[DEBUG] Set of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + ors.getSet());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + ors.getBuffer());
            });
        });
    }

    @Unit(at = "1")
    public void test2(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                ORSet<Integer> ors = new ORSet<>("one", dataManager);

                for (int i = 10; i <= 15; i++) {

                    ors.applyOperation(new SetOperation<>(SetOperation.ADD, i, UUID.randomUUID()), dataManager);
                }

                Thread.sleep(500);

               for (int i = 5; i <= 18; i++) {
                    if(ors.getId(i) != null) {
                        ors.applyOperation(new SetOperation<>(SetOperation.REMOVE, i, ors.getId(i)), dataManager);
                    }
                }

                ors.finish(dataManager);

                System.out.println("[DEBUG] Set of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + ors.getSet());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + ors.getBuffer());
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
                .run(ORSetTestJob.class, 1)
                .done();
    }
}
