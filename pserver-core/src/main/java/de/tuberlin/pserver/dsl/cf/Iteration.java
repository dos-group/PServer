package de.tuberlin.pserver.dsl.cf;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.InstanceContext;
import de.tuberlin.pserver.math.Matrix;

public final class Iteration {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final InstanceContext instanceContext;

    private int epoch;

    private boolean isGlobalSynced;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public Iteration(final InstanceContext instanceContext) {
        this.instanceContext = Preconditions.checkNotNull(instanceContext);
    }

    // ---------------------------------------------------
    // Synchronization.
    // ---------------------------------------------------

    public Iteration sync() { isGlobalSynced = true; return this; }

    public Iteration async() { isGlobalSynced = false; return this; }

    // ---------------------------------------------------
    // Execution.
    // ---------------------------------------------------

    public void execute(final IterationTermination t, final Body b) {

        while (!t.terminate()) {

            b.body();

            if (isGlobalSynced && instanceContext.instanceID == 0)
                instanceContext.jobContext.dataManager.globalSync();

            ++epoch;
        }
    }

    public void execute(final int n, final Body b) {

        for (epoch = 0; epoch < n; ++epoch) {

            b.body();

            if (isGlobalSynced && instanceContext.instanceID == 0)
                instanceContext.jobContext.dataManager.globalSync();
        }
    }

    public void execute(final Matrix m, final RowMatrixIterationBody b) {

        final Matrix.RowIterator iter = instanceContext.jobContext.dataManager.createThreadPartitionedRowIterator(m);

        while (iter.hasNextRow()) {

            iter.nextRow();

            b.body(iter);

            if (isGlobalSynced && instanceContext.instanceID == 0)
                instanceContext.jobContext.dataManager.globalSync();

            ++epoch;
        }
    }
}
