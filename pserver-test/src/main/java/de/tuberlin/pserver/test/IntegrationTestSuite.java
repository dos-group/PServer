package de.tuberlin.pserver.test;

import de.tuberlin.pserver.node.PServerMain;
import de.tuberlin.pserver.runtime.core.config.Config;
import de.tuberlin.pserver.runtime.core.config.ConfigLoader;
import de.tuberlin.pserver.runtime.core.infra.ClusterSimulator;
import de.tuberlin.pserver.test.core.CoreTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({CoreTests.class})
public class IntegrationTestSuite {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final int NUM_NODES = 4;

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public static ClusterSimulator clusterSimulator = null;

    public static boolean isRunning = false;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public IntegrationTestSuite() {
    }

    // --------------------------------------------------
    // Test Suite Methods.
    // --------------------------------------------------

    @BeforeClass
    public static void setIsRunningTrue() {
        isRunning = true;
    }

    @BeforeClass
    public static void setUpTestEnvironment() {
        System.setProperty("simulation.numNodes", String.valueOf(NUM_NODES));
        final Config simConfig = ConfigLoader.loadResource("local.config");
        clusterSimulator = new ClusterSimulator(simConfig, false, PServerMain.class);
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