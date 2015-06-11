package de.tuberlin.pserver.examples.graphs;

import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.math.Matrix;

public final class TarjanSCCJob extends PServerJob {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {
        dataManager.loadDMatrix("datasets/graph.dat");
    }

    @Override
    public void compute() {

        final Matrix m = dataManager.getLocalMatrix("graph.dat");

        LOG.info("row: " + m.numRows() + ", col: " + m.numCols());

        //final TarjanSCC scc = new TarjanSCC(graphData);

        //final int count = scc.count();

        //LOG.info("==> Strongly Connected Components: " + count);
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        PServerExecutor.LOCAL
                .run(TarjanSCCJob.class)
                //.results(weights)
                .done();
    }
}