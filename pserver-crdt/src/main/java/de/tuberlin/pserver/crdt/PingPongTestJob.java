package de.tuberlin.pserver.crdt;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.ExecutionManager;
import de.tuberlin.pserver.runtime.MLProgram;


public class PingPongTestJob extends MLProgram {

    public static final int NUM_MSG = 50;

    @Override
    public void define(final Program program) {

        program.process(() -> {

            CF.parScope().node(0).slot(0).exe(() -> { // node 0 with slot 0 executes this section...

                for (int i = 0; i < NUM_MSG; ++i) {

                    dataManager.pushTo("ping", new Integer(i), new int[] { 1 });

                    dataManager.awaitEvent(ExecutionManager.CallType.SYNC, 1, "pong", new DataManager.DataEventHandler() {
                        @Override
                        public void handleDataEvent(int srcNodeID, Object value) {
                            final Integer i = (Integer)value;
                            System.out.println("received pong " + i);
                        }
                    });
                }

                System.out.println("-- FINISH NODE " + slotContext);
            });

            // ---------------------------------------------------

            CF.parScope().node(1).slot(0).exe(() -> { // node 1 with slot 0 executes this section...

                for (int i = 0; i < NUM_MSG; ++i) {

                    dataManager.awaitEvent(ExecutionManager.CallType.SYNC, 1, "ping", new DataManager.DataEventHandler() {
                        @Override
                        public void handleDataEvent(int srcNodeID, Object value) {
                            final Integer i = (Integer)value;
                            System.out.println("received ping " + i);
                        }
                    });

                    dataManager.pushTo("pong", new Integer(i), new int[] { 0 });
                }

                System.out.println("-- FINISH NODE " + slotContext);
            });
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", "2");
        // Set the memory each simulated node gets.
        //System.setProperty("jvmOptions", "[\"-Xmx1024m\"]");

        PServerExecutor.LOCAL
                // Second param is number of slots (threads executing the job) per node,
                // should be 1 at the beginning.
                .run(PingPongTestJob.class, 1)
                .done();
    }
}