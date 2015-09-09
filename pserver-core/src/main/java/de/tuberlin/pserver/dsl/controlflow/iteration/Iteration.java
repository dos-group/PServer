package de.tuberlin.pserver.dsl.controlflow.iteration;

import de.tuberlin.pserver.dsl.controlflow.CFStatement;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.runtime.ExecutionManager;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.SlotGroup;

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

    private final ExecutionManager executionManager;

    private long epoch;

    private int mode = ASYNC;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public Iteration(final SlotContext sc) {
        super(sc);

        this.executionManager = sc.programContext.runtimeContext.executionManager;
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

            final SlotGroup sg = executionManager.getActiveSlotGroup();

            long duration, passDuration = 0, syncDuration = 0;

            duration = System.currentTimeMillis();

            while (!t.terminate()) {

                long t0 = System.currentTimeMillis();

                long t1 = System.currentTimeMillis();

                //sg.sync(slotContext);

                sync(sg);

                syncDuration += System.currentTimeMillis() - t1;

                b.body(epoch);

                ++epoch;

                passDuration += System.currentTimeMillis() - t0;
            }

            duration = System.currentTimeMillis() - duration;

            setProfilingData(new IterationProfilingData(duration, passDuration / epoch, syncDuration / epoch));

        leave();

        return this;
    }

    // ---------------------------------------------------
    // Matrix Operations.
    // ---------------------------------------------------

    public Iteration parExe(final Matrix m, final MatrixRowIterationBody b) throws Exception { return exe(parallelMatrixRowIterator(m), b); }

    public Iteration exe(final Matrix m, final MatrixRowIterationBody b)throws Exception { return exe(m.rowIterator(), b); }

    public Iteration exe(final Matrix.RowIterator ri, final MatrixRowIterationBody b) throws Exception {
        final IterationBody ib = (epoch) -> b.body(epoch, ri);
        final IterationTermination it = () -> {
                final boolean t = !ri.hasNext();
                if (!t) ri.next();
                return t;
        };
        return exe(it, ib);
    }

    // ---------------------------------------------------

    public Iteration parExe(final Matrix m, final MatrixElementIterationBody b) throws Exception { return parExe(m, toRowMatrixIB(m, b)); }

    public Iteration exe(final Matrix m, final MatrixElementIterationBody b) throws Exception { return exe(m, toRowMatrixIB(m, b)); }

    private MatrixRowIterationBody toRowMatrixIB(final Matrix m, final MatrixElementIterationBody b) throws Exception {
        return (epoch, rit) -> {
            for (long j = 0; j < m.cols(); ++j) {
                b.body(epoch, rit.rowNum(), j, rit.value((int) j));
            }
        };
    }

    // ---------------------------------------------------
    // Vector Operations.
    // ---------------------------------------------------

    public Iteration parExe(final Vector v, final VectorElementIterationBody b) throws Exception { return exe(parallelVectorElementIterator(v), b); }

    public Iteration exe(final Vector v, final VectorElementIterationBody b)throws Exception { return exe(v.elementIterator(), b); }

    public Iteration exe(final Vector.ElementIterator ei, final VectorElementIterationBody b) throws Exception {
        final IterationBody ib = (epoch) -> b.body(epoch, ei);
        final IterationTermination it = () -> {
            final boolean t = !ei.hasNextElement();
            if (!t) ei.nextElement();
            return t;
        };
        return exe(it, ib);
    }

    // ---------------------------------------------------

    public long getEpoch() { return epoch; }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void sync(final SlotGroup sg) throws Exception {
        switch (mode) {
            case ASYNC:
                return;
            case LOCAL:
                sg.sync(slotContext);
                return;
            case GLOBAL:
                sg.sync(slotContext);
                if (sg.minSlotID == slotContext.slotID)
                    executionManager.globalSync(slotContext);
                return;
        }
    }

    private Matrix.RowIterator parallelMatrixRowIterator(final Matrix m) {
        return executionManager.parallelMatrixRowIterator(m);
    }

    private Vector.ElementIterator parallelVectorElementIterator(final Vector v) {
        return executionManager.parallelVectorElementIterator(v);
    }
}
