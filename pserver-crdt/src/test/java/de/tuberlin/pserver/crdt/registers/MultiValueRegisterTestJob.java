package de.tuberlin.pserver.crdt.registers;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;

// TODO: this only works to a precision of milliseconds (Date class)!
// Is it ok to use System.nanoTime in multicore systems? http://www.principiaprogramatica.com/?p=16
// TODO: This needs more testing and validation
// TODO: if datamanager was serializable, I wouldn't need to pass it to all these damn functions...
// TODO: better soution for the getRegister method to return a set
public class MultiValueRegisterTestJob extends Program {

    @Unit(at = "0")
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            MultiValueRegister<Integer> mvr = new MultiValueRegister<>("one", 2, programContext);

            for (int i = 0; i <= 10000; i++) {
                mvr.set(i);
            }

            mvr.finish();

            System.out.println("[DEBUG] Register of node " + programContext.runtimeContext.nodeID + ": "
                    + mvr.getRegister());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                    + mvr.getBuffer());
        });
    }

    @Unit(at = "1")
    public void test2(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            MultiValueRegister<Integer> mvr = new MultiValueRegister<>("one", 2, programContext);

            for (int i = 0; i <= 1000; i++) {
                mvr.set(i);
            }

            mvr.finish();

            System.out.println("[DEBUG] Register of node " + programContext.runtimeContext.nodeID + ": "
                    + mvr.getRegister());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                    + mvr.getBuffer());
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
                .run(MultiValueRegisterTestJob.class)
                .done();
    }
}
