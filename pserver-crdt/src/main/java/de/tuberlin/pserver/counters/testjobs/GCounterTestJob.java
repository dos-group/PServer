package de.tuberlin.pserver.counters.testjobs;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.counters.CounterOperation;
import de.tuberlin.pserver.counters.GCounter;
import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.MLProgram;

public class GCounterTestJob extends MLProgram {


    @Unit(at = "0")
    public void test(Program program) {
        program.process(() -> {
            CF.parUnit(0).exe(() -> {
                GCounter gc = new GCounter("one", dataManager);
                GCounter gc2 = new GCounter("two", dataManager);

                for (int i = 0; i < 10000; i++) {
                    gc.applyOperation(new CounterOperation(CounterOperation.ADD, 1), dataManager);
                    if ((i % 2) == 0) {
                        gc2.applyOperation(new CounterOperation(CounterOperation.ADD, 1), dataManager);
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
            CF.parUnit(0).exe(() -> {
                GCounter gc = new GCounter("one", dataManager);
                GCounter gc2 = new GCounter("two", dataManager);

                for (int i = 0; i < 100000; i++) {
                    gc.applyOperation(new CounterOperation(CounterOperation.ADD, 1), dataManager);
                    if ((i % 2) == 0) {
                        gc2.applyOperation(new CounterOperation(CounterOperation.ADD, 1), dataManager);
                    }
                }

                gc.finish(dataManager);
                gc2.finish(dataManager);

                System.out.println("[DEBUG] Count of node " + slotContext.programContext.runtimeContext.nodeID + ": " + gc.getCount());
                System.out.println("[DEBUG] Count of node " + slotContext.programContext.runtimeContext.nodeID + ": " + gc2.getCount());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID + ": " + gc.getBuffer());
                System.out.println("[DEBUG] Buffer of node " + slotContext.programContext.runtimeContext.nodeID + ": " + gc2.getBuffer());
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
                .run(GCounterTestJob.class, 1)
                .done();
    }
}
