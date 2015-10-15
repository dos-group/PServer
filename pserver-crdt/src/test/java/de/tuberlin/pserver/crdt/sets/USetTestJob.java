package de.tuberlin.pserver.crdt.sets;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;

// TODO: this needs more testing and debugging + cleanup + testing for throwing an exception
public class USetTestJob extends Program {

    @Unit(at = "0")
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
                USet<Integer> us = new USet<>("one", runtimeManager);

                for (int i = 0; i <= 10; i++) {
                    us.add(i);
                }

                us.finish();

                System.out.println("[DEBUG] Set of node " + programContext.runtimeContext.nodeID + ": " + us.getSet());
                System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + us.getBuffer());
            });
    }

    @Unit(at = "1")
    public void test2(Lifecycle lifecycle) {
        lifecycle.process(() -> {
                USet<Integer> us = new USet<>("one", runtimeManager);

                for (int i = 16; i <= 20; i++) {
                    us.add(i);
                }

                Thread.sleep(500);

                for (int i = 5; i <= 10; i++) {
                    us.remove(i);
                }

                us.finish();

                System.out.println("[DEBUG] Set of node " + programContext.runtimeContext.nodeID + ": " + us.getSet());
                System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + us.getBuffer());
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
                .run(USetTestJob.class)
                .done();
    }
}
