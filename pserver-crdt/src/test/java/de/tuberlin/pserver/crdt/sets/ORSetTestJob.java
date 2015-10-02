package de.tuberlin.pserver.crdt.sets;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.MLProgram;

// TODO: this needs testing and debugging and major cleanup
public class ORSetTestJob extends MLProgram {

    @Unit(at = "0")
    public void test(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                ORSet<Integer> ors = new ORSet<>("one", dataManager);

                for (int i = 0; i < 10; i++) {
                    //ors.applyOperation(new SetOperation<>(CRDT.ADD, i, UUID.randomUUID()), dataManager);
                    ors.add(i, dataManager);
                }

                //ors.applyOperation(new SetOperation<>(CRDT.ADD, 7, UUID.randomUUID()), dataManager);
                ors.add(7, dataManager);

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

                    //ors.applyOperation(new SetOperation<>(CRDT.ADD, i, UUID.randomUUID()), dataManager);
                    ors.add(i, dataManager);
                }

                Thread.sleep(500);

               for (int i = 5; i <= 18; i++) {
                    if(ors.getId(i) != null) {
                        //ors.applyOperation(new SetOperation<>(CRDT.REMOVE, i, ors.getId(i)), dataManager);
                        ors.remove(i, dataManager);
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
