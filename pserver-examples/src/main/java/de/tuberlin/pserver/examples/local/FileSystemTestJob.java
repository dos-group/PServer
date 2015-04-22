package de.tuberlin.pserver.examples.local;

import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.app.types.DMatrix;
import de.tuberlin.pserver.client.PServerExecutor;

import java.text.DecimalFormat;


public final class FileSystemTestJob extends PServerJob {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {
        dataManager.loadDMatrix("datasets/data1.csv");
    }

    @Override
    public void compute() {

        final DMatrix data  = dataManager.getLocalMatrix("data1.csv");

        final DMatrix.RowIterator iter = data.rowIterator();

        final DecimalFormat numberFormat = new DecimalFormat("###.###");

        if (ctx.instanceID == 0) {
            while (iter.hasNextRow()) {
                for (int i = 0; i < iter.numCols(); ++i) {
                    System.out.print(numberFormat.format(iter.getValue(i)) + "\t\t");
                }
                System.out.println();
                iter.nextRow();
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