package de.tuberlin.pserver.test.core.programs;

import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.runtime.MsgEventHandler;
import de.tuberlin.pserver.runtime.RuntimeManager;

public class SendReceiveTestJob extends Program {

    public static final int NUM_MSG = 20000;

    @Unit(at = "0")
    public void pingNode(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            Thread.sleep(3000);

            for (int i = 0; i < NUM_MSG; ++i) {

                runtimeManager.send("test-ping", i, new int[]{1});

                runtimeManager.receive(RuntimeManager.ReceiveType.SYNC, 1, "test-pong", new MsgEventHandler() {

                    @Override
                    public void handleMsg(int srcNodeID, Object value) {
                    }
                });
            }

            System.out.println("-- FINISH NODE 0");
        });
    }

    @Unit(at = "1")
    public void pongNode(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            for (int i = 0; i < NUM_MSG; ++i) {

                runtimeManager.receive(RuntimeManager.ReceiveType.SYNC, 1, "test-ping", new MsgEventHandler() {

                    @Override
                    public void handleMsg(int srcNodeID, Object value) {
                    }
                });

                runtimeManager.send("test-pong", i, new int[]{0});
            }

            System.out.println("-- FINISH NODE 1");
        });
    }
}