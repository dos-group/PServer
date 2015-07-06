package de.tuberlin.pserver.examples.playground;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.app.filesystem.record.IRecordFactory;
import de.tuberlin.pserver.app.filesystem.record.RecordFormat;
import de.tuberlin.pserver.app.partitioning.MatrixByRowParitioner;
import de.tuberlin.pserver.app.types.MatrixEntry;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.examples.ml.GenerateLocalTestData;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.math.stuff.Utils;
import de.tuberlin.pserver.ml.models.GeneralLinearModel;
import de.tuberlin.pserver.ml.optimization.*;
import de.tuberlin.pserver.ml.optimization.SGD.SGDOptimizer;

import java.io.*;
import java.text.DecimalFormat;
import java.util.List;

public final class ThreadedMatrixLoaderTestJob extends PServerJob {

    private static final int NUM_NODES = 4;

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

    private boolean isOwnPartition(int row, int col, int numNodes) {
        double numOfRowsPerInstance = (double) GenerateLocalTestData.ROWS_ROWCOLVAL_DATASET / NUM_NODES;
        double partition = row / numOfRowsPerInstance;
        return Utils.toInt((long) (partition % NUM_NODES)) == ctx.instanceID;
    }

    @Override
    public void compute() {
        final Matrix matrix = dataManager.getObject("datasets/rowcolval_dataset.csv");
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader("datasets/rowcolval_dataset.csv"));
            String line = null;
            while((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                int row = Integer.parseInt(parts[0]);
                int col = Integer.parseInt(parts[1]);
                double val = Double.parseDouble(parts[2]);
                if(isOwnPartition(row, col, NUM_NODES)) {
                    double matrixVal = matrix.get(row, col);
                    if(matrixVal != val) {
                        //throw new RuntimeException(ctx.instanceID + ": matrix("+row+","+col+") is "+matrixVal+" but should be "+val);
                        System.out.println(ctx.instanceID + ": matrix("+row+","+col+") is "+matrixVal+" but should be "+val);
                    }
                    assert(matrixVal == val);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            if(br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<List<Serializable>> res = Lists.newArrayList();

        System.setProperty("simulation.numNodes", String.valueOf(NUM_NODES));

        PServerExecutor.LOCAL
                .run(ThreadedMatrixLoaderTestJob.class, 1) // <-- enable multi-threading, 2 threads per compute node.
                .results(res)
                .done();
    }
}