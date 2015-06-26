package de.tuberlin.pserver.examples.playground;

import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.math.Matrix;

import java.util.Random;

public final class AsyncMatrixPullTestJob extends PServerJob {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int MTX_ROWS = 2000;

    private static final int MTX_COLS = 2000;

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Random rand = new Random();

    private final DataManager.Merger<Matrix> merger = (localMatrix, remoteMatrices) -> {
        for (int i = 0; i < localMatrix.numRows(); ++i) {
            for (int j = 0; j < localMatrix.numCols(); ++j) {
                double v = 0.0;
                for (final Matrix m : remoteMatrices)
                    v += m.get(i, j);
                localMatrix.set(i, j, (v / remoteMatrices.length));
            }
        }
    };

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {
    }

    @Override
    public void compute() {

       final Matrix matrix = dataManager.createLocalMatrix("model1", MTX_ROWS, MTX_COLS);

       for (int i = 0; i < 9000; ++i) {
            randomUpdate(matrix);
            if (i % 1000 == 0)
                ctx.dataManager.mergeMatrix(matrix, merger);
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void randomUpdate(final Matrix matrix) {
        int x = rand.nextInt(MTX_ROWS - 1);
        int y = rand.nextInt(MTX_COLS - 1);
        double v = rand.nextDouble();
        matrix.set(x, y, v);
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        PServerExecutor.LOCAL
                .run(AsyncMatrixPullTestJob.class)
                .done();
    }
}