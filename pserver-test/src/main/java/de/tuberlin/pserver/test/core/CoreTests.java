package de.tuberlin.pserver.test.core;


import de.tuberlin.pserver.client.PServerClient;
import de.tuberlin.pserver.client.PServerClientFactory;
import de.tuberlin.pserver.test.IntegrationTestSuite;
import de.tuberlin.pserver.test.core.jobs.AggregatorTestJob;
import de.tuberlin.pserver.test.core.jobs.SharedVarTestJob;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoreTests {

    // --------------------------------------------------
    // Fields.
    // --------------------------------------------------

    private static PServerClient client;

    private static int numSlots;

    // --------------------------------------------------
    // Tests.
    // --------------------------------------------------

    @BeforeClass
    public static void setup() {
        if (!IntegrationTestSuite.isRunning)
            IntegrationTestSuite.setUpTestEnvironment();
        client = new PServerClient(PServerClientFactory.INSTANCE);
        numSlots = Integer.parseInt(System.getProperty("simulation.numSlots"));
    }

    //@Test
    //public void testMatrixLoading() { assert client.execute(MatrixLoadingTestJob.class, numSlots) != null; }

    @Test
    public void testAggregator() { assert client.execute(AggregatorTestJob.class, numSlots) != null; }

    @Test
    public void testSharedVar() { assert client.execute(SharedVarTestJob.class, numSlots) != null; }

    @AfterClass
    public static void tearDown() {
        client.deactivate();
        if (!IntegrationTestSuite.isRunning) {
            IntegrationTestSuite.tearDownTestEnvironment();
        }
    }
}
