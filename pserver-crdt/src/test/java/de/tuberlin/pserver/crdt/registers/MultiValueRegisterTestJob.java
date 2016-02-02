package de.tuberlin.pserver.crdt.registers;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class MultiValueRegisterTestJob extends Program {
    private static final int NUM_NODES = 2;
    private static final int NUM_REPLICAS = 2;

    private static final String CRDT_ID = "one";

    @Unit
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            MultiValueRegister<Integer> register = new MultiValueRegister<>(CRDT_ID, NUM_REPLICAS, programContext);

            Set<Integer> testSet = new HashSet<>();

            for (int i = 0; i <= 10; i++) {
                testSet.add(i);
            }

            register.set(testSet);
            testSet.clear();

            for (int i = 5; i <= 15; i++) {
                testSet.add(i);
            }
            register.set(testSet);

            register.finish();
            result(register.get().toArray());

            /*
            System.out.println("[DEBUG] Register of node " + programContext.runtimeContext.nodeID + ": " + register.get());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + register.getBuffer());
            */
        });
    }

    public static void main(final String[] args) {
        final List<List<Serializable>> results = Lists.newArrayList();

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", String.valueOf(NUM_NODES));
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx256m\"]");

        PServerExecutor.LOCAL
                .run(MultiValueRegisterTestJob.class)
                .results(results)
                .done();

        // Compare results of the CRDTs
        assertEquals("The resulting CRDTs are not identical",
                ((Object[])results.get(0).get(0)).length,
                ((Object[])results.get(1).get(0)).length);

        Object[] results1 = ((Object[])results.get(0).get(0));
        Object[] results2 = ((Object[])results.get(1).get(0));

        for(int i = 0; i < ((Object[])results.get(0).get(0)).length; i++) {
            assertEquals("The resulting CRDTs are not identical", results1[i], results2[i]);
        }
    }
}
