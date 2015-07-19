package de.tuberlin.pserver.examples.playground.primitives;


import com.google.common.util.concurrent.AtomicDouble;
import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.client.PServerExecutor;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class ASyncPushPrimitiveTestJob extends PServerJob {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    final AtomicDouble localValue  = new AtomicDouble(0.0);

    final AtomicDouble globalValue = new AtomicDouble(0.0);

    final AtomicInteger receiveCnt = new AtomicInteger(0);

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

                dataManager.awaitEvent(DataManager.CallType.ASYNC, "localValue", new DataManager.DataEventHandler() {

                    @Override
                    public void handleDataEvent(int srcInstanceID, Object value) {
                        globalValue.addAndGet((Double)value);
                        receiveCnt.incrementAndGet();
                    }
                });

                // active polling.
                while (receiveCnt.get() != dataManager.getNumberOfInstances() - 1) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {}
                }

                LOG.info("GLOBAL VALUE = " + globalValue.get());

                receiveCnt.set(0);
            }
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        PServerExecutor.LOCAL
                .run(ASyncPushPrimitiveTestJob.class)
                .done();
    }
}
