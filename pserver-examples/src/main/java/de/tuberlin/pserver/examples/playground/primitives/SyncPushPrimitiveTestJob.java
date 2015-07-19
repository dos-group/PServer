package de.tuberlin.pserver.examples.playground.primitives;


import com.google.common.util.concurrent.AtomicDouble;
import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.client.PServerExecutor;

import java.util.Random;

public class SyncPushPrimitiveTestJob extends PServerJob {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    final AtomicDouble localValue  = new AtomicDouble(0.0);

    final AtomicDouble globalValue = new AtomicDouble(0.0);

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void compute() {

        while (true) {

            localValue.set(0.0);

            globalValue.set(0.0);

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {}

            localValue.set(ctx.instanceID * 10.0);

            if (ctx.instanceID != 0) {

                dataManager.pushTo("localValue", localValue.get(), new int[] { 0 });

            } else {

                dataManager.awaitEvent(DataManager.CallType.SYNC, "localValue", new DataManager.DataEventHandler() {

                    @Override
                    public void handleDataEvent(int srcInstanceID, Object value) {
                        globalValue.addAndGet((Double)value);
                    }
                });

                LOG.info("GLOBAL VALUE = " + globalValue.get());
            }
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        PServerExecutor.LOCAL
                .run(SyncPushPrimitiveTestJob.class)
                .done();
    }
}
