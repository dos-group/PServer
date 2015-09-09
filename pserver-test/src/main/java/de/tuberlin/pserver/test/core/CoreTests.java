package de.tuberlin.pserver.test.core;


import de.tuberlin.pserver.client.PServerClient;
import de.tuberlin.pserver.client.PServerClientFactory;
import de.tuberlin.pserver.test.IntegrationTestSuite;
import de.tuberlin.pserver.test.core.programs.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoreTests {

    // --------------------------------------------------
    // Fields.
    // --------------------------------------------------

    private static PServerClient client;

    // --------------------------------------------------
    // Tests.
    // --------------------------------------------------

    @BeforeClass
    public static void setup() {
        if (!IntegrationTestSuite.isRunning)
            IntegrationTestSuite.setUpTestEnvironment();
        client = new PServerClient(PServerClientFactory.INSTANCE);
    }

    // --------------------------------------------------

    @Test
    public void testPushAwait() { assert client.execute(PushAwaitTestJob.class, IntegrationTestSuite.NUM_SLOTS) != null; }

    @Test
    public void testEventSystemSendReceive() { assert client.execute(EventSystemSendReceiveTestJob.class, IntegrationTestSuite.NUM_SLOTS) != null; }

    @Test
    public void testLocalSync() { assert client.execute(LocalSyncTestJob.class, IntegrationTestSuite.NUM_SLOTS) != null; }

    @Test
    public void testSharedVar() { assert client.execute(SharedVarTestJob.class, IntegrationTestSuite.NUM_SLOTS) != null; }

    @Test
    public void testSelectControlFlow() { assert client.execute(SelectControlFlowTestJob.class, IntegrationTestSuite.NUM_SLOTS) != null; }

    @Test
    public void testGlobalSync() { assert client.execute(GlobalSyncTestJob.class, IntegrationTestSuite.NUM_SLOTS) != null; }

    @Test
    public void testSymmetricAggregator() { assert client.execute(SymAggregatorTestJob.class, IntegrationTestSuite.NUM_SLOTS) != null; }

    @Test
    public void testAsymmetricAggregator() { assert client.execute(ASymAggregatorTestJob.class, IntegrationTestSuite.NUM_SLOTS) != null; }

    //@Test
    //public void testMatrixDenseLoading() { assert client.execute(MatrixDenseLoadingTestJob.class, IntegrationTestSuite.NUM_SLOTS) != null; }

    //@Test
    //public void testMatrixSparseLoading() { assert client.execute(MatrixSparseLoadingTestJob.class, IntegrationTestSuite.NUM_SLOTS) != null; }

    // --------------------------------------------------

    @AfterClass
    public static void tearDown() {
        client.deactivate();
        if (!IntegrationTestSuite.isRunning) {
            IntegrationTestSuite.tearDownTestEnvironment();
        }
    }
}
