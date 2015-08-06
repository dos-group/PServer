package de.tuberlin.pserver.dsl.controlflow;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.InstanceContext;
import de.tuberlin.pserver.math.matrix.Matrix;

public final class Iteration {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final int ASYNC   = 1;

    public static final int GLOBAL  = 2;

    public static final int LOCAL   = 4;

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final InstanceContext instanceContext;

    private long epoch;

    private int mode = ASYNC;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public Iteration(final InstanceContext instanceContext) {
        this.instanceContext = Preconditions.checkNotNull(instanceContext);
    }

    // ---------------------------------------------------
    // Synchronization.
    // ---------------------------------------------------

    public Iteration sync(int mode) { this.mode = mode; return this; }

    // ---------------------------------------------------
    // Execution.
    // ---------------------------------------------------

    public void execute(final IterationTermination t, final IterationBody b) {

        while (!t.terminate()) {

            b.body(epoch);

            sync();

            ++epoch;
        }
    }

    public void execute(final long n, final IterationBody b) {

        for (epoch = 0; epoch < n; ++epoch) {

            b.body(epoch);

            sync();
        }
    }

    public void executePartitioned(final Matrix m, final RowMatrixIterationBody b) {

        final Matrix.RowIterator iter = instanceContext.jobContext.dataManager.createThreadPartitionedRowIterator(m);

        while (iter.hasNextRow()) {

            iter.nextRow();

            b.body(iter);

            sync();

            ++epoch;
        }
    }

    public void execute(final Matrix m, final RowMatrixIterationBody b) {

        final Matrix.RowIterator iter = m.rowIterator();

        while (iter.hasNextRow()) {

            iter.nextRow();

            b.body(iter);

            sync();

            ++epoch;
        }
    }

    public void execute(final Matrix m, final MatrixElementIterationBody b) {

        final Matrix.RowIterator iter = m.rowIterator();

        while (iter.hasNextRow()) {

            iter.nextRow();

            for (long j = 0; j < m.numCols(); ++j) {

                b.body(epoch, j, iter.getValueOfColumn((int)j));
            }

            sync();

            ++epoch;
        }
    }

    public long getEpoch() { return epoch; }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void sync() {
        if (!((mode & ASYNC) == ASYNC))
            if ((mode & LOCAL) == LOCAL)
                instanceContext.jobContext.dataManager.localSync();
            if ((mode & GLOBAL) == GLOBAL && instanceContext.instanceID == 0)
                instanceContext.jobContext.dataManager.globalSync();
    }
}
