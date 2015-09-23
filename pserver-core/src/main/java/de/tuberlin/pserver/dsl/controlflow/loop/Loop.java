package de.tuberlin.pserver.dsl.controlflow.loop;

import de.tuberlin.pserver.dsl.controlflow.base.CFStatement;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.SlotGroup;

public final class Loop extends CFStatement {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final int ASYNC   = 1;

    public static final int GLOBAL  = 2;

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static final class IterationProfilingData extends ProfilingData {

        public final long passDuration;

        public final long syncDuration;

        public IterationProfilingData(final long duration, final long passDuration, final long syncDuration) {
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

    public Loop(final SlotContext sc) {
        super(sc);

        this.dataManager = sc.runtimeContext.dataManager;
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

            //sync(sg);

            syncDuration += System.currentTimeMillis() - t1;

            b.body(epoch);

            ++epoch;

            passDuration += System.currentTimeMillis() - t0;
        }

        duration = System.currentTimeMillis() - duration;

        setProfilingData(new IterationProfilingData(duration, epoch > 0 ? passDuration / epoch : passDuration, epoch > 0 ? syncDuration / epoch : syncDuration));

        //leave();

        return this;
    }

    // ---------------------------------------------------
    // Matrix Operations.
    // ---------------------------------------------------

    /*public Loop parExe(final Matrix m, final MatrixRowIteratorBody b) throws Exception { return exe(parallelMatrixRowIterator(m), b); }

    public Loop exe(final Matrix m, final MatrixRowIteratorBody b)throws Exception {
        return exe(m.rowIterator(), b);
    }

    public Loop exe(final Matrix.RowIterator ri, final MatrixRowIteratorBody b) throws Exception {
        final LoopBody ib = (epoch) -> b.body(epoch, ri);
        final LoopTermination it = () -> {
                final boolean t = !ri.hasNext();
                if (!t) ri.next();
                return t;
        };
        return exe(it, ib);
    }*/

    // ---------------------------------------------------

    /*public Loop parExe(final Matrix m, final MatrixElementIteratorBody b) throws Exception { return parExe(m, toRowMatrixIB(m, b)); }

    public Loop exe(final Matrix m, final MatrixElementIteratorBody b) throws Exception { return exe(m, toRowMatrixIB(m, b)); }

    private MatrixRowIteratorBody toRowMatrixIB(final Matrix m, final MatrixElementIteratorBody b) throws Exception {
        return (epoch, rit) -> {
            for (long j = 0; j < m.cols(); ++j) {
                b.body(epoch, rit.rowNum(), j, rit.value((int) j));
            }
        };
    }*/

    // ---------------------------------------------------

    public long getEpoch() { return epoch; }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void sync(final SlotGroup sg) throws Exception {
        switch (mode) {
            case ASYNC:
                return;
            case GLOBAL:
                if (sg.minSlotID == slotContext.slotID)
                    dataManager.globalSync(slotContext);
        }
    }

    //private Matrix.RowIterator parallelMatrixRowIterator(final Matrix m) {
    //    return executionManager.parallelMatrixRowIterator(m);
    //}
}
