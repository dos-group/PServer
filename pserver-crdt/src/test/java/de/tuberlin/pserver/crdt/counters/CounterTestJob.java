package de.tuberlin.pserver.crdt.counters;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.MLProgram;

public class CounterTestJob extends MLProgram {

    @Unit(at = "0")
    public void test(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                Counter gc = new Counter("one", dataManager);
                Counter gc2 = new Counter("two", dataManager);

                for (int i = 0; i < 10000; i++) {
                    gc.subtract(1);

                    if ((i % 2) == 0) {
                        gc2.subtract(1);
                    }
                }

                gc.finish(dataManager);
                gc2.finish(dataManager);

                System.out.println("[DEBUG] Count of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + gc.getCount());
                System.out.println("[DEBUG] Count of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + gc2.getCount());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + gc.getBuffer());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + gc2.getBuffer());
            });
        });
    }

    @Unit(at = "1")
    public void test2(Program program) {
        program.process(() -> {
            CF.parUnit(1).slot(0).exe(() -> {
                Counter gc = new Counter("one", dataManager);
                Counter gc2 = new Counter("two", dataManager);

                for (int i = 0; i < 50000; i++) {
                    gc.add(1);

                    if ((i % 2) == 0) {
                        gc2.add(1);
                    }
                }

                gc.finish(dataManager);
                gc2.finish(dataManager);

                System.out.println("[DEBUG] Count of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + gc.getCount());
                System.out.println("[DEBUG] Count of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + gc2.getCount());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + gc.getBuffer());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID +
                        " slot " + slotContext.slotID + ": " + gc2.getBuffer());
            });
        });
    }

    public static void main(final String[] args) {

        // ISet the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", "2");
        // ISet the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx512m\"]");

        PServerExecutor.LOCAL
                // Second param is number of slots (threads executing the job) per node,
                // should be 1 at the beginning.
                .run(CounterTestJob.class, 1)
                .done();
    }
}
