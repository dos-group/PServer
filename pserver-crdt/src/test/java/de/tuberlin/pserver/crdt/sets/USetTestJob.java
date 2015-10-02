package de.tuberlin.pserver.crdt.sets;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.MLProgram;

// TODO: this needs more testing and debugging + cleanup + testing for throwing an exception
public class USetTestJob extends MLProgram {

    @Unit(at = "0")
    public void test(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                USet<Integer> us = new USet<>("one", dataManager);

                for (int i = 0; i <= 10; i++) {
                    us.add(i);
                }

                us.finish(dataManager);

                System.out.println("[DEBUG] ISet of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + us.getSet());
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
                    us.add(i);
                }

                Thread.sleep(500);

                for (int i = 5; i <= 10; i++) {
                    us.remove(i);
                }

                us.finish(dataManager);

                System.out.println("[DEBUG] ISet of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + us.getSet());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + us.getBuffer());
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
                .run(USetTestJob.class, 1)
                .done();
    }
}
