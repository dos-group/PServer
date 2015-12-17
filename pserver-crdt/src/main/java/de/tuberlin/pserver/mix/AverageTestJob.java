package de.tuberlin.pserver.mix;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AverageTestJob extends Program {
    private final String NUM_NODES = "2";

    @Unit(at = "0")
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            AverageMix m = new AverageMix("one", 2, programContext);

            for(int i = 1; i < 10; i++) {
                m.average(i);
                System.out.println("[DEBUG] Avg of " + programContext.runtimeContext.nodeID + ": " + m.getAvg());
            }

            m.incSessionCount();
            m.newSession();

            for(int i = 21; i < 30; i++) {
                m.average(i);
                System.out.println("[DEBUG] Avg of " + programContext.runtimeContext.nodeID + ": " + m.getAvg());
            }


            m.finish();

            System.out.println("[DEBUG] Final avg of " + programContext.runtimeContext.nodeID + ": " + m.getAvg());


            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + m.getBuffer());

        });
    }

    @Unit(at = "1")
    public void test2(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            AverageMix m = new AverageMix("one", 2, programContext);

            for(int i = 11; i < 20; i++) {
                m.average(i);
                System.out.println("[DEBUG] Avg of " + programContext.runtimeContext.nodeID + ": " + m.getAvg());
            }

            m.incSessionCount();
            m.newSession();

            for(int i = 41; i < 50; i++) {
                m.average(i);
                System.out.println("[DEBUG] Avg of " + programContext.runtimeContext.nodeID + ": " + m.getAvg());
            }


            m.finish();

            System.out.println("[DEBUG] Final avg of " + programContext.runtimeContext.nodeID + ": " + m.getAvg());


            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + m.getBuffer());
        });
    }

    @Test
    public void main() {

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", NUM_NODES);
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx256m\"]");

        PServerExecutor.LOCAL
                // Second param is number of slots (threads executing the job) per node,
                // should be 1 at the beginning.
                .run(AverageTestJob.class)
                .done();
    }
}
