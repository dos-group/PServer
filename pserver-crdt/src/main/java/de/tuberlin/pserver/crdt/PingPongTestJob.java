package de.tuberlin.pserver.crdt;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Lifecycle;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.MLProgram;


public class PingPongTestJob extends MLProgram {

    public static final int NUM_MSG = 50;

    @Unit(at = "0")
    public void pingNode(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            for (int i = 0; i < NUM_MSG; ++i) {

                dataManager.pushTo("ping", new Integer(i), new int[]{1});

                dataManager.awaitEvent(DataManager.CallType.SYNC, 1, "pong", new DataManager.DataEventHandler() {
                    @Override
                    public void handleDataEvent(int srcNodeID, Object value) {
                        final Integer i = (Integer) value;
                        System.out.println("received pong " + i);
                    }
                });
            }

            System.out.println("-- FINISH NODE " + slotContext);

        });
    }

    @Unit(at = "1")
    public void pongNode(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            for (int i = 0; i < NUM_MSG; ++i) {

                dataManager.awaitEvent(DataManager.CallType.SYNC, 1, "ping", new DataManager.DataEventHandler() {
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
                .run(PingPongTestJob.class)
                .done();
    }
}