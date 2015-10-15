package de.tuberlin.pserver.crdt.registers;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;

// TODO: this only works to a precision of milliseconds (Date class)!
// Is it ok to use System.nanoTime in multicore systems? http://www.principiaprogramatica.com/?p=16
public class LWWRegisterTestJob extends Program {

    @Unit(at = "0")
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
                LWWRegister<Integer> lwwr = new LWWRegister<>("one", runtimeManager, (i1, i2) -> i1 > i2);

                for (int i = 0; i <= 10000; i++) {
                    lwwr.set(i);
                }

                lwwr.finish();

                System.out.println("[DEBUG] Register of node " + programContext.runtimeContext.nodeID + ": "
                        + lwwr.getRegister());
                System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                        + lwwr.getBuffer());
        });
    }

    @Unit(at = "1")
    public void test2(Lifecycle lifecycle) {
        lifecycle.process(() -> {
                LWWRegister<Integer> lwwr = new LWWRegister<>("one", runtimeManager, (i1, i2) -> i1 > i2);

                for (int i = 0; i <= 10; i++) {
                    lwwr.set(i);
                    Thread.sleep(500);
                }

                lwwr.finish();

                System.out.println("[DEBUG] Register of node " + programContext.runtimeContext.nodeID + ": "
                        + lwwr.getRegister());
                System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                        + lwwr.getBuffer());
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
                .run(LWWRegisterTestJob.class)
                .done();
    }
}
