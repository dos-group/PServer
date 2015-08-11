package de.tuberlin.pserver.examples.playground;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.runtime.MLProgram;

import java.util.Random;

public class CFSyncPrimitiveTestJob extends MLProgram {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {

    }

    @Override
    public void compute() {

        int i = 0;

        final Random rand = new Random();

        while (true) {

            LOG.info("node [" + slotContext.programContext.runtimeContext.nodeID + "]: " + i);

            try {
                Thread.sleep((long)(rand.nextDouble() * 5000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            i++;

            executionManager.globalSync();
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        PServerExecutor.LOCAL
                .run(CFSyncPrimitiveTestJob.class)
                .done();
    }
}

