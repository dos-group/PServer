package de.tuberlin.pserver.examples.playground;

import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.examples.ml.GenerateLocalTestData;
import de.tuberlin.pserver.math.matrix.Matrix;

import java.text.DecimalFormat;


public final class FileSystemTestJob extends PServerJob {

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

        final Matrix.RowIterator iter = data.rowIterator();

        final DecimalFormat numberFormat = new DecimalFormat("###.###");

        if (instanceContext.jobContext.nodeID == 0) {
            while (iter.hasNextRow()) {
                iter.nextRow();
                for (int i = 0; i < iter.numCols(); ++i) {
                    System.out.print(numberFormat.format(iter.getValueOfColumn(i)) + "\t\t");
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
                .run(FileSystemTestJob.class)
                .done();
    }
}