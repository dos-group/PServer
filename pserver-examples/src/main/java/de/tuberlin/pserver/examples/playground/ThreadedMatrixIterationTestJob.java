package de.tuberlin.pserver.examples.playground;

import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.examples.ml.GenerateLocalTestData;
import de.tuberlin.pserver.math.Matrix;

import java.text.DecimalFormat;

public final class ThreadedMatrixIterationTestJob extends PServerJob {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {
        dataManager.loadAsMatrix("datasets/demo_dataset.csv", GenerateLocalTestData.ROWS_DEMO_DATASET, GenerateLocalTestData.COLS_DEMO_DATASET);
    }

    @Override
    public void compute() {

        final Matrix data = dataManager.getObject("demo_dataset.csv");

        final Matrix.RowIterator iter = dataManager.createThreadPartitionedRowIterator(data);

        if (ctx.instanceID == 0 && ctx.threadID == 1) {
            final DecimalFormat numberFormat = new DecimalFormat("0.000");
            while (iter.hasNextRow()) {
                iter.nextRow();
                for (int i = 0; i < iter.numCols(); ++i) {
                    System.out.print(numberFormat.format(iter.getValueOfColumn(i)) + "\t | ");
                }
                System.out.println();
            }
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        PServerExecutor.LOCAL
                .run(ThreadedMatrixIterationTestJob.class, 4)
                .done();
    }
}