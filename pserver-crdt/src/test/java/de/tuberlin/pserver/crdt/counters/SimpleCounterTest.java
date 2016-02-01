package de.tuberlin.pserver.crdt.counters;


import de.tuberlin.pserver.client.PServerClient;
import de.tuberlin.pserver.client.PServerClientFactory;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.node.PServerMain;
import de.tuberlin.pserver.runtime.core.config.IConfig;
import de.tuberlin.pserver.runtime.core.config.IConfigFactory;
import de.tuberlin.pserver.runtime.core.infra.ClusterSimulator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

// TODO: sometimes the test seems to stall at the finish() method

public class SimpleCounterTest {
    private static final int NUM_NODES = 3;
    private static ClusterSimulator clusterSimulator;
    private static PServerClient client;

    @BeforeClass
    public static void setup() {
        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", String.valueOf(NUM_NODES));

        //TODO: this shouldn't be here
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx256m\"]");

        final IConfig simConfig = IConfigFactory.load(IConfig.Type.PSERVER_SIMULATION);
        clusterSimulator = new ClusterSimulator(simConfig, false, PServerMain.class);

        client = new PServerClient(PServerClientFactory.INSTANCE);
    }

    @AfterClass
    public static void tearDown() {
        if(client != null) {
            client.deactivate();
        }
        if(clusterSimulator != null) {
            clusterSimulator.deactivate();
        }
    }

    //*****************************************************

    @Test
    public void initializedCounterShouldBeZero() {
        assertNotNull(client.execute(InitializedCounterShouldBeZeroTestJob.class));
    }

    public static class InitializedCounterShouldBeZeroTestJob extends Program {
        @Unit(at = "0")
        public void test(Lifecycle lifecycle) {
            lifecycle.process(() -> {
                SimpleCounter gc = SimpleCounter.newReplica("counter", 2, programContext);
                assertEquals("An initialized SimpleCounter should have count 0", 0L, gc.getCount());
                gc.finish();
            });
        }

        @Unit(at = "1")
        public void test2(Lifecycle lifecycle) {
            lifecycle.process(() -> {
                SimpleCounter gc = SimpleCounter.newReplica("counter", 2, programContext);
                assertEquals("An initialized SimpleCounter should have count 0", 0L, gc.getCount());
                gc.finish();
            });
        }
    }

    //*****************************************************

    @Test
    public void incrementShouldIncreaseCount() {
        assertNotNull(client.execute(IncrementShouldIncreaseCountTestJob.class));
    }

    public static class IncrementShouldIncreaseCountTestJob extends Program {
        @Unit(at = "0")
        public void test(Lifecycle lifecycle) {
            lifecycle.process(() -> {
                SimpleCounter gc = SimpleCounter.newReplica("counter", 2, programContext);
                gc.increment(10);
                assertEquals("Increment should increase the counter", 10, gc.getCount());
                gc.finish();
            });
        }

        @Unit(at = "1")
        public void test2(Lifecycle lifecycle) {
            lifecycle.process(() -> {
                SimpleCounter gc = SimpleCounter.newReplica("counter", 2, programContext);
                gc.increment(10);
                assertEquals("Increment should increase the counter", 10, gc.getCount());
                gc.finish();
            });
        }
    }
    //*****************************************************

    @Test
    public void decrementShouldDecreaseCount() {
        assertNotNull(client.execute(DecrementShouldDecreaseCountTestJob.class));
    }

    public static class DecrementShouldDecreaseCountTestJob extends Program {
        @Unit(at = "0")
        public void test(Lifecycle lifecycle) {
            lifecycle.process(() -> {
                SimpleCounter gc = SimpleCounter.newReplica("counter", 2, programContext);
                gc.decrement(10);
                assertEquals("Decrement should decrease the counter", -10L, gc.getCount());
                gc.finish();
            });
        }

        @Unit(at = "1")
        public void test2(Lifecycle lifecycle) {
            lifecycle.process(() -> {
                SimpleCounter gc = SimpleCounter.newReplica("counter", 2, programContext);
                gc.decrement(10);
                assertEquals("Decrement should decrease the counter", -10L, gc.getCount());
                gc.finish();
            });
        }
    }
    //*****************************************************

    @Test
    public void incrementsShouldBeBroadcastAndApplied() {
        assertNotNull(client.execute(IncrementsShouldBeBroadcastAndAppliedTestJob.class));
    }

    public static class IncrementsShouldBeBroadcastAndAppliedTestJob extends Program {
        @Unit(at = "0")
        public void test(Lifecycle lifecycle) {
            lifecycle.process(() -> {
                SimpleCounter gc = SimpleCounter.newReplica("counter", 2, programContext);
                gc.increment(10);
                gc.finish();
                assertEquals("Increment should increase the counter", 10, gc.getCount());
            });
        }

        @Unit(at = "1")
        public void test2(Lifecycle lifecycle) {
            lifecycle.process(() -> {
                SimpleCounter gc = SimpleCounter.newReplica("counter", 2, programContext);
                gc.finish();
                assertEquals("Counter should have received and applied broadcast increment", 10, gc.getCount());
            });
        }
    }
    //*****************************************************

    @Test
    public void deccrementsShouldBeBroadcastAndApplied() {
        assertNotNull(client.execute(DecrementsShouldBeBroadcastAndAppliedTestJob.class));
    }

    public static class DecrementsShouldBeBroadcastAndAppliedTestJob extends Program {
        @Unit(at = "0")
        public void test(Lifecycle lifecycle) {
            lifecycle.process(() -> {
                SimpleCounter gc = SimpleCounter.newReplica("counter", 2, programContext);
                gc.decrement(10);
                gc.finish();
                assertEquals("Decrement should decrease the counter", -10, gc.getCount());
            });
        }

        @Unit(at = "1")
        public void test2(Lifecycle lifecycle) {
            lifecycle.process(() -> {
                SimpleCounter gc = SimpleCounter.newReplica("counter", 2, programContext);
                gc.finish();
                assertEquals("Counter should have received and applied broadcast decrement", -10, gc.getCount());
            });
        }
    }
}
