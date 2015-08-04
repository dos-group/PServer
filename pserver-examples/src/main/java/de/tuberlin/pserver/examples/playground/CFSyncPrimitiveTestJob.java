package de.tuberlin.pserver.examples.playground;

import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.client.PServerExecutor;

import java.util.Random;

public class CFSyncPrimitiveTestJob extends PServerJob {

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

            LOG.info("node [" + instanceContext.jobContext.nodeID + "]: " + i);

            try {
                Thread.sleep((long)(rand.nextDouble() * 5000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            i++;

            dataManager.globalSync(0);
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

