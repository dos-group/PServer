package de.tuberlin.pserver.test;

import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.config.IConfigFactory;
import de.tuberlin.pserver.core.infra.ClusterSimulator;
import de.tuberlin.pserver.node.PServerMain;
import de.tuberlin.pserver.test.core.CoreTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runners.Suite;
import org.junit.runner.RunWith;

@RunWith(Suite.class)
@Suite.SuiteClasses({CoreTests.class})
public class IntegrationTestSuite {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final int NUM_NODES = 4;

    public static final int NUM_SLOTS = 4;

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public static ClusterSimulator clusterSimulator = null;

    public static boolean isRunning = false;

    // --------------------------------------------------
    // Test Suite Methods.
    // --------------------------------------------------

    @BeforeClass
    public static void setIsRunningTrue() {
        isRunning = true;
    }

    @BeforeClass
    public static void setUpTestEnvironment() {
        final IConfig simConfig = IConfigFactory.load(IConfig.Type.PSERVER_SIMULATION);
        System.setProperty("simulation.numNodes", String.valueOf(NUM_NODES));
        System.setProperty("simulation.numSlots", String.valueOf(NUM_SLOTS));
        clusterSimulator = new ClusterSimulator(simConfig, PServerMain.class);
    }

    @AfterClass
    public static void setIsRunningFalse() {
        isRunning = false;
    }

    @AfterClass
    public static void tearDownTestEnvironment() {
        if (clusterSimulator != null) {
            clusterSimulator.deactivate();
        }
    }
}