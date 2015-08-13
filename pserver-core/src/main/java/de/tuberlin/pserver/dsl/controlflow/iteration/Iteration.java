package de.tuberlin.pserver.dsl.controlflow.iteration;

import de.tuberlin.pserver.dsl.controlflow.CFStatement;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.SlotContext;

public final class Iteration extends CFStatement {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final int ASYNC   = 1;

    public static final int GLOBAL  = 2;

    public static final int LOCAL   = 4;

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

    private long epoch;

    private int mode = ASYNC;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public Iteration(final SlotContext ic) {
        super(ic);
    }

    // ---------------------------------------------------
    // Synchronization.
    // ---------------------------------------------------

    public Iteration sync(int mode) { this.mode = mode; return this; }

    // ---------------------------------------------------
    // Execution.
    // ---------------------------------------------------

    public Iteration exe(final long n, final IterationBody b) throws Exception { return exe(() -> !(epoch < n), b); }

    public Iteration exe(final IterationTermination t, final IterationBody b) throws Exception {

        enter();

        long duration, passDuration = 0, syncDuration = 0;

        duration = System.currentTimeMillis();
        
        while (!t.terminate()) {

            long t0 = System.currentTimeMillis();
            
            b.body(epoch);

            long t1 = System.currentTimeMillis();
            
            sync();

            syncDuration += System.currentTimeMillis() - t1;

            ++epoch;

            passDuration += System.currentTimeMillis() - t0;
        }

        duration = System.currentTimeMillis() - duration;

        setProfilingData(new IterationProfilingData(duration, passDuration / epoch, syncDuration / epoch));

        leave();

        return this;
    }

    // ---------------------------------------------------

    public Iteration parExe(final Matrix m, final RowMatrixIterationBody b) throws Exception { return exe(makeParIterator(m), b); }

    public Iteration exe(final Matrix m, final RowMatrixIterationBody b)throws Exception {
        return exe(m.rowIterator(), b);
    }

    public Iteration exe(final Matrix.RowIterator ri, final RowMatrixIterationBody b) throws Exception {
        final IterationBody ib = (epoch) -> b.body(epoch, ri);
        final IterationTermination it = () -> {
                final boolean t = !ri.hasNextRow();
                if (!t) ri.nextRow();
                return t;
        };
        return exe(it, ib);
    }

    // ---------------------------------------------------

    private RowMatrixIterationBody toRowMatrixIB(final Matrix m, final MatrixElementIterationBody b) throws Exception {
        return (epoch, rit) -> {
            for (long j = 0; j < m.numCols(); ++j)
                b.body(epoch, epoch, j, rit.getValueOfColumn((int) j));
        };
    }

    public Iteration parExe(final Matrix m, final MatrixElementIterationBody b) throws Exception { return parExe(m, toRowMatrixIB(m, b)); }

    public Iteration exe(final Matrix m, final MatrixElementIterationBody b) throws Exception { return exe(m, toRowMatrixIB(m, b)); }

    // ---------------------------------------------------

    public long getEpoch() { return epoch; }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void sync() {
        if (!((mode & ASYNC) == ASYNC))
            if ((mode & LOCAL) == LOCAL)
                slotContext.programContext.runtimeContext.executionManager.localSync();
            if ((mode & GLOBAL) == GLOBAL && slotContext.slotID == 0)
                slotContext.programContext.runtimeContext.executionManager.globalSync();
    }

    private Matrix.RowIterator makeParIterator(final Matrix m) {
        return slotContext.programContext.runtimeContext.executionManager.parRowIterator(m);
    }
}
