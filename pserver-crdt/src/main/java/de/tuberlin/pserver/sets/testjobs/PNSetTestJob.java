package de.tuberlin.pserver.sets.testjobs;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.MLProgram;
import de.tuberlin.pserver.sets.PNSet;

// TODO: this needs testing and debugging
public class PNSetTestJob extends MLProgram {

    @Unit(at = "0")
    public void test(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                PNSet<Integer> pns = new PNSet<>("one", dataManager);

                for (int i = 0; i < 10; i++) {
                    pns.add(i, dataManager);
                }

                pns.finish(dataManager);

                System.out.println("[DEBUG] Set of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + pns.getSet());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + pns.getBuffer());
            });
        });
    }

    @Unit(at = "1")
    public void test2(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                PNSet<Integer> pns = new PNSet<>("one", dataManager);

                for (int i = 10; i <= 15; i++) {
                    pns.add(i, dataManager);
                }

                Thread.sleep(500);

                for (int i = 5; i <= 11; i++) {
                    pns.remove(i, dataManager);
                }

                pns.finish(dataManager);

                System.out.println("[DEBUG] Set of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + pns.getSet());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + pns.getBuffer());
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
                .run(PNSetTestJob.class, 1)
                .done();
    }
}
