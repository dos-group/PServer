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

    // TODO: Every test is running for its own, but running the whole integration suite stalls.
    // TODO: Some test programs must interfere due to not properly cleaning program and runtime stateName after termination.

    @Test
    public void testUnitControlFlow() { assert client.execute(UnitControlFlowTestJob.class) != null; }

    @Test
    public void testSingletonMatrix() { assert client.execute(SingletonMatrixTestJob.class) != null; }

    @Test
    public void testSendReceive() { assert client.execute(SendReceiveTestJob.class) != null; }

    @Test
    public void testEventSystemSendReceive() { assert client.execute(EventSystemSendReceiveTestJob.class) != null; }

    @Test
    public void testSharedVar() { assert client.execute(SharedVarTestJob.class) != null; }

    @Test
    public void testGlobalSync() { assert client.execute(GlobalSyncTestJob.class) != null; }

    @Test
    public void testSymmetricAggregator() { assert client.execute(SymAggregatorTestJob.class) != null; }

    @Test
    public void testAsymmetricAggregator() { assert client.execute(ASymAggregatorTestJob.class) != null; }

    @Test
    public void testMatrixDenseLoadingRowColVal() { assert client.execute(MatrixDenseLoadingRowColValTestJob.class) != null; }

    @Test
    public void testMatrixDenseLoadingRow() { assert client.execute(MatrixDenseLoadingRowTestJob.class) != null; }

    @Test
    public void testMatrixSparseLoading() { assert client.execute(MatrixSparseLoadingTestJob.class) != null; }

    // --------------------------------------------------

    @AfterClass
    public static void tearDown() {
        client.deactivate();
        if (!IntegrationTestSuite.isRunning) {
            IntegrationTestSuite.tearDownTestEnvironment();
        }
    }
}
