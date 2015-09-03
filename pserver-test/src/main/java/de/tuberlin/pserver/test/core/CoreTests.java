package de.tuberlin.pserver.test.core;


import de.tuberlin.pserver.client.PServerClient;
import de.tuberlin.pserver.client.PServerClientFactory;
import de.tuberlin.pserver.test.IntegrationTestSuite;
import de.tuberlin.pserver.test.core.jobs.*;
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
    //public void testPushAwait() { assert client.execute(PushAwaitTestJob.class, numSlots) != null; }

    //@Test
    //public void testMatrixLoading() { assert client.execute(MatrixLoadingTestJob.class, numSlots) != null; }

    //@Test
    //public void testSymmetricAggregator() { assert client.execute(SymAggregatorTestJob.class, numSlots) != null; }

    //@Test
    //public void testAsymmetricAggregator() { assert client.execute(ASymAggregatorTestJob.class, numSlots) != null; }

    //@Test
    //public void testSharedVar() { assert client.execute(SharedVarTestJob.class, numSlots) != null; }

    @Test
    public void testSelectControlFlow() { assert client.execute(SelectControlFlowTestJob.class, numSlots) != null; }

    @AfterClass
    public static void tearDown() {
        client.deactivate();
        if (!IntegrationTestSuite.isRunning) {
            IntegrationTestSuite.tearDownTestEnvironment();
        }
    }
}
