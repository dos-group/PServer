package de.tuberlin.pserver.test.core.programs;

import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.MLProgram;

public class PushAwaitTestJob extends MLProgram {

    public static final int NUM_MSG = 20000;

    @Unit(at = "0")
    public void pingNode(final Program program) {

        program.process(() -> {

            Thread.sleep(3000);

            for (int i = 0; i < NUM_MSG; ++i) {

                dataManager.pushTo("test-ping", i, new int[]{1});

                dataManager.awaitEvent(DataManager.CallType.SYNC, 1, "test-pong", new DataManager.DataEventHandler() {
                    @Override
                    public void handleDataEvent(int srcNodeID, Object value) {
                    }
                });
            }

            System.out.println("-- FINISH NODE 0");
        });
    }

    @Unit(at = "1")
    public void pongNode(final Program program) {

        program.process(() -> {

            for (int i = 0; i < NUM_MSG; ++i) {

                dataManager.awaitEvent(DataManager.CallType.SYNC, 1, "test-ping", new DataManager.DataEventHandler() {
                    @Override
                    public void handleDataEvent(int srcNodeID, Object value) {}
                });

                dataManager.pushTo("test-pong", i, new int[] { 0 });
            }

            System.out.println("-- FINISH NODE 1");
        });
    }
}