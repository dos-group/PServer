package de.tuberlin.pserver.dsl.unit.controlflow.loop;

import de.tuberlin.pserver.dsl.unit.controlflow.base.CFStatement;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.ProgramContext;

public final class Loop extends CFStatement {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final int ASYNC   = 1;

    public static final int GLOBAL = 2;

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static final class LoopProfilingData extends ProfilingData {

        public final long passDuration;

        public final long syncDuration;

        public LoopProfilingData(final long duration, final long passDuration, final long syncDuration) {
            super(duration);
            this.passDuration = passDuration;
            this.syncDuration = syncDuration;
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final DataManager dataManager;

    private long epoch;

    private int mode = ASYNC;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public Loop(final ProgramContext pc) {
        super(pc);

        this.dataManager = pc.runtimeContext.dataManager;
    }

    // ---------------------------------------------------
    // Synchronization.
    // ---------------------------------------------------

    public Loop sync(int mode) { this.mode = mode; return this; }

    // ---------------------------------------------------
    // Execution.
    // ---------------------------------------------------

    public Loop exe(final long n, final LoopBody b) throws Exception { return exe(() -> !(epoch < n), b); }

    public Loop exe(final LoopTermination t, final LoopBody b) throws Exception {

        //enter();

        //final SlotGroup sg = executionManager.getActiveSlotGroup();

        long duration, passDuration = 0, syncDuration = 0;

        duration = System.currentTimeMillis();

        while (!t.terminate()) {

            long t0 = System.currentTimeMillis();

            long t1 = System.currentTimeMillis();

            sync();

            syncDuration += System.currentTimeMillis() - t1;

            b.body(epoch);

            ++epoch;

            passDuration += System.currentTimeMillis() - t0;
        }

        duration = System.currentTimeMillis() - duration;

        setProfilingData(new LoopProfilingData(duration, epoch > 0 ? passDuration / epoch : passDuration, epoch > 0 ? syncDuration / epoch : syncDuration));

        //leave();

        return this;
    }

    // ---------------------------------------------------

    public long getEpoch() { return epoch; }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void sync() throws Exception {
        switch (mode) {
            case ASYNC:
                return;
            case GLOBAL:
                dataManager.globalSync(programContext);
        }
    }
}
