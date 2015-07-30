package de.tuberlin.pserver.app;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.events.Event;

import java.util.concurrent.CyclicBarrier;

public abstract class IterativeProgram extends PServerJob {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String OBSERVER_PERIOD_EVENT = "observer_period_event";

    // ---------------------------------------------------

    public static enum SynchronizationMode {

        BULK_SYNCHRONOUS_PARALLEL,

        ASYNCHRONOUS

        //STALE_SYNCHRONOUS_PARALLEL, // Not supported at the moment.
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static CyclicBarrier internalSyncBarrier;

    private int epoch;

    private int numIterations;

    private int observerPeriod;

    private SynchronizationMode externalSyncMode;

    private SynchronizationMode internalSyncMode;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public IterativeProgram() {
        super();

        if (ctx.threadID == 0) {
            internalSyncBarrier = new CyclicBarrier(ctx.perNodeParallelism);
        }

        this.epoch = 0;

        this.numIterations = -1;

        this.observerPeriod = -1;

        this.externalSyncMode = SynchronizationMode.ASYNCHRONOUS;

        this.internalSyncMode = SynchronizationMode.ASYNCHRONOUS;
    }

    // ---------------------------------------------------
    // Public Method.
    // ---------------------------------------------------

    public boolean terminate() { return true; }

    public abstract void iterate();

    // ---------------------------------------------------

    @Override
    public void prologue() {}

    @Override
    public void compute() {

        while (((epoch < numIterations) || (numIterations == -1)) && terminate()) {

            iterate();

            if (epoch % observerPeriod == 0) {
                dispatchEvent(new Event(OBSERVER_PERIOD_EVENT));
            }

            ++epoch;

            if (externalSyncMode == SynchronizationMode.BULK_SYNCHRONOUS_PARALLEL) {
                ctx.dataManager.sync(0);
            } else if (internalSyncMode == SynchronizationMode.BULK_SYNCHRONOUS_PARALLEL) {
                try {
                    internalSyncBarrier.await();
                } catch (Exception e) {
                    LOG.error(e.getLocalizedMessage());
                }
            }
        }
    }

    @Override
    public void epilogue() {}

    // ---------------------------------------------------

    public void internalSync(final SynchronizationMode mode) { this.internalSyncMode = Preconditions.checkNotNull(mode); }

    public void externalSync(final SynchronizationMode mode) { this.externalSyncMode = Preconditions.checkNotNull(mode); }

    public void numIterations(int numIterations) { this.numIterations = numIterations; }

    public void observerPeriod(final int observerPeriod) { this.observerPeriod = observerPeriod; }
}
