package de.tuberlin.pserver.crdt;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.runtime.events.MsgEventHandler;
import de.tuberlin.pserver.runtime.RuntimeManager;


public class PingPongTestJob extends Program {

    public static final int NUM_MSG = 50;

    @Unit(at = "0")
    public void pingNode(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            for (int i = 0; i < NUM_MSG; ++i) {

                runtimeManager.send("ping", new Integer(i), new int[]{1});

                runtimeManager.receive(RuntimeManager.ReceiveType.SYNC, 1, "pong", new MsgEventHandler() {
                    @Override
                    public void handleMsg(int srcNodeID, Object value) {
                        final Integer i = (Integer) value;
                        System.out.println("received pong " + i);
                    }
                });
            }

            System.out.println("-- FINISH NODE " + programContext);

        });
    }

    @Unit(at = "1")
    public void pongNode(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            for (int i = 0; i < NUM_MSG; ++i) {

                runtimeManager.receive(RuntimeManager.ReceiveType.SYNC, 1, "ping", new MsgEventHandler() {
                    @Override
                    public void handleMsg(int srcNodeID, Object value) {
                        final Integer i = (Integer) value;
                        System.out.println("received ping " + i);
                    }
                });

                runtimeManager.send("pong", new Integer(i), new int[]{0});
            }

            System.out.println("-- FINISH NODE " + programContext);
        });
    }


    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        // Set the number of simulated at, can also be
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