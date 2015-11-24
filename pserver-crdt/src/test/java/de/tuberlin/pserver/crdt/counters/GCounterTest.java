package de.tuberlin.pserver.crdt.counters;


import de.tuberlin.pserver.client.PServerClient;
import de.tuberlin.pserver.client.PServerClientFactory;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.node.PServerMain;
import de.tuberlin.pserver.runtime.core.config.IConfig;
import de.tuberlin.pserver.runtime.core.config.IConfigFactory;
import de.tuberlin.pserver.runtime.core.infra.ClusterSimulator;
import org.junit.*;

import static org.junit.Assert.*;

// TODO: sometimes the test seems to stall at the finish() method
// TODO: what about incrementing by a negative value?
// Todo: what about decrementing by a negative value?
// TODO: what about incrementing by null?

public class GCounterTest {
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
                GCounter gc = new GCounter("counter", 2, programContext);
                assertEquals("An initialized GCounter should have count 0", 0L, gc.getCount());
                gc.finish();
            });
        }

        @Unit(at = "1")
        public void test2(Lifecycle lifecycle) {
            lifecycle.process(() -> {
                GCounter gc = new GCounter("counter", 2, programContext);
                assertEquals("An initialized GCounter should have count 0", 0L, gc.getCount());
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
                GCounter gc = new GCounter("counter", 2, programContext);
                System.out.println("*");
                gc.increment(10);
                System.out.println("**");
                assertEquals("Increment should increase the counter", 10, gc.getCount());
                System.out.println("***");
                gc.finish();
                System.out.println("****");
            });
        }

        @Unit(at = "1")
        public void test2(Lifecycle lifecycle) {
            lifecycle.process(() -> {
                GCounter gc = new GCounter("counter", 2, programContext);
                System.out.println("O");
                gc.increment(10);
                System.out.println("OO");
                assertEquals("Increment should increase the counter", 10, gc.getCount());
                System.out.println("OOO");
                gc.finish();
                System.out.println("OOOO");
            });
        }
    }
    //*****************************************************

    @Test
    public void incrementByNegativeValueShouldThrowIllegalArgumentException() {
        assertNotNull(client.execute(IncrementByNegativeValueShouldThrowIllegalArgumentException.class));
    }

    public static class IncrementByNegativeValueShouldThrowIllegalArgumentException extends Program {
        @Unit(at = "0")
        public void test(Lifecycle lifecycle) {
            lifecycle.process(() -> {
                boolean failed = false;
                GCounter gc = new GCounter("counter", 2, programContext);
                try {
                    gc.increment(-10);
                }
                catch(IllegalArgumentException ex) {
                    failed = true;
                }
                gc.finish();
                assertTrue(failed);
            });
        }

        @Unit(at = "1")
        public void test2(Lifecycle lifecycle) {
            lifecycle.process(() -> {
                boolean failed = false;
                GCounter gc = new GCounter("counter", 2, programContext);
                try {
                    gc.increment(-10);
                } catch (IllegalArgumentException ex) {
                    failed = true;
                }
                gc.finish();
                assertTrue(failed);
            });
        }
    }
    //*****************************************************

   @Test
    public void decrementShouldThrowUnsupportedOperationException() {
        assertNotNull(client.execute(DecrementShouldThrowUnsupportedOperationExceptionTestJob.class));
    }

    public static class DecrementShouldThrowUnsupportedOperationExceptionTestJob extends Program {
        @Unit(at = "0")
        public void test(Lifecycle lifecycle) {
            lifecycle.process(() -> {
                boolean failed = false;
                System.out.println("*");
                GCounter gc = new GCounter("counter", 2, programContext);
                System.out.println("**");

                try {
                    gc.decrement(10);
                    System.out.println("***");

                }
                catch(UnsupportedOperationException ex) {
                    failed = true;
                    System.out.println("****");

                }
                gc.finish();
                System.out.println("*****");

                assertTrue(failed);
                System.out.println("******");

            });
        }

        @Unit(at = "1")
        public void test2(Lifecycle lifecycle) {
            lifecycle.process(() -> {
                boolean failed = false;
                GCounter gc = new GCounter("counter", 2, programContext);
                try {
                    gc.decrement(10);
                }
                catch(UnsupportedOperationException ex) {
                    failed = true;
                }
                gc.finish();
                assertTrue(failed);
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
                GCounter gc = new GCounter("counter", 2, programContext);
                gc.increment(10);
                gc.finish();
                assertEquals("Increment should increase the counter", 10, gc.getCount());
            });
        }

        @Unit(at = "1")
        public void test2(Lifecycle lifecycle) {
            lifecycle.process(() -> {
                GCounter gc = new GCounter("counter", 2, programContext);
                gc.finish();
                assertEquals("Counter should have received broadcast increment", 10, gc.getCount());
            });
        }
    }
}
