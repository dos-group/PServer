package de.tuberlin.pserver.examples.playground;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.app.filesystem.record.IRecordFactory;
import de.tuberlin.pserver.app.filesystem.record.RecordFormat;
import de.tuberlin.pserver.app.partitioning.MatrixByRowParitioner;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.examples.ml.GenerateLocalTestData;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.ml.models.GeneralLinearModel;
import de.tuberlin.pserver.ml.optimization.*;
import de.tuberlin.pserver.ml.optimization.SGD.SGDOptimizer;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;

public final class ThreadedMatrixLoaderTestJob extends PServerJob {


    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {

        dataManager.loadAsMatrix(
                "datasets/rowcolval_dataset.csv",
                GenerateLocalTestData.ROWS_ROWCOLVAL_DATASET,
                GenerateLocalTestData.COLS_ROWCOLVAL_DATASET,
                RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD)
        );

    }

    @Override
    public void compute() {
        final Matrix data = dataManager.getObject("datasets/rowcolval_dataset.csv");
        String out = "";
        for(long i = 0; i < data.numRows(); i++) {
            for(int j = 0; j < data.numCols(); j++) {
                out += "("+i+","+j+","+data.get(i, j)+")";
            }
        }
        System.out.println(Thread.currentThread().getName() + ":" + out);

    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<List<Serializable>> res = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(ThreadedMatrixLoaderTestJob.class, 1) // <-- enable multi-threading, 2 threads per compute node.
                .results(res)
                .done();
    }
}