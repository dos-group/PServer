package de.tuberlin.pserver.crdt.registers;


import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import org.junit.Test;

import java.io.Serializable;
import java.util.Calendar;
import java.util.List;
import java.util.Random;


import static org.junit.Assert.assertEquals;

public class LWWRegisterTestJob extends Program {
    private static final int NUM_NODES = 2;
    private static final int NUM_REPLICAS = 2;

    private static final String CRDT_ID = "one";

    @Unit
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            LWWRegister<Integer> register = new LWWRegister<>(CRDT_ID, NUM_REPLICAS, programContext); //, (i1, i2) -> i1 > i2);
            Random rand = new Random(Calendar.getInstance().getTimeInMillis());

            for (int i = 0; i <= 10000; i++) {
                register.set(i);
                if(rand.nextFloat() > 0.9) {
                    Thread.sleep(10);
                }
            }

            register.finish();

            result(register.get(), register.getTimestamp());


            System.out.println("[DEBUG] Register of node " + programContext.runtimeContext.nodeID + ": " + register.get());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": " + register.getBuffer());
            System.out.println("[DEBUG] Timestamp of node " + programContext.runtimeContext.nodeID + ": " + register.getTimestamp());

        });
    }

    @Test
    public static void main(final String[] args) {
        final List<List<Serializable>> results = Lists.newArrayList();

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", String.valueOf(NUM_NODES));
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx256m\"]");

        PServerExecutor.LOCAL
                .run(LWWRegisterTestJob.class)
                .results(results)
                .done();

        // Compare results of the CRDTs
        assertEquals("The resulting CRDTs are not identical", results.get(0).get(0), results.get(1).get(0));
        assertEquals("The resulting CRDTs are not identical", results.get(0).get(1), results.get(1).get(1));
    }
}
