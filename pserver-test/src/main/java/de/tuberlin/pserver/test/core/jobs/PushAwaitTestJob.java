package de.tuberlin.pserver.test.core.jobs;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.ExecutionManager;
import de.tuberlin.pserver.runtime.MLProgram;

import java.io.Serializable;

public class PushAwaitTestJob extends MLProgram {

    public static final int NUM_MSG = 1000;

    public static class TestObject implements Serializable {

        public final String name;

        public final int ID;

        public TestObject(final String name, final int ID) {

            this.name = name;

            this.ID = ID;
        }
    }

    @Override
    public void define(final Program program) {

        final DataManager dataManager = slotContext.programContext.runtimeContext.dataManager;

        program.process(() -> {

            Thread.sleep(3000);

            CF.select().node(0).slot(0).exe(() -> {

                for (int i = 0; i < NUM_MSG; ++i) {

                    dataManager.pushTo("ping", new TestObject("ping-Msg", 0), new int[]{1});

                    dataManager.awaitEvent(ExecutionManager.CallType.SYNC, 1, "pong", new DataManager.DataEventHandler() {
                        @Override
                        public void handleDataEvent(int srcNodeID, Object value) {
                            final TestObject to = (TestObject) value;
                            //System.out.println("MachineNr = " + srcNodeID + ",  Name = " + to.name);
                        }
                    });
                }

                System.out.println("FINISH - NODE 0");
            });

            CF.select().node(1).slot(0).exe(() -> {

                for (int i = 0; i < NUM_MSG; ++i) {

                    dataManager.awaitEvent(ExecutionManager.CallType.SYNC, 1, "ping", new DataManager.DataEventHandler() {
                        @Override
                        public void handleDataEvent(int srcNodeID, Object value) {
                            final TestObject to = (TestObject) value;
                            //System.out.println("MachineNr = " + srcNodeID + ",  Name = " + to.name);
                        }
                    });

                    dataManager.pushTo("pong", new TestObject("pong-Msg", 0), new int[]{0});
                }

                System.out.println("FINISH - NODE 1");
            });
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        PServerExecutor.LOCAL
                .run(PushAwaitTestJob.class, 1)
                .done();
    }

}
