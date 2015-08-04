package de.tuberlin.pserver.examples.playground;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.app.filesystem.record.IRecordFactory;
import de.tuberlin.pserver.app.filesystem.record.RecordFormat;
import de.tuberlin.pserver.app.partitioning.MatrixByRowPartitioner;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.examples.ml.GenerateLocalTestData;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.stuff.Utils;

import java.io.*;
import java.util.List;

public final class ThreadedMatrixLoaderTestJob extends PServerJob {

    private static final int NUM_NODES = 4;

    private static final String FILE = "datasets/rowcolval_dataset_10000_2500.csv";

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {

        dataManager.loadAsMatrix(
                FILE,
                GenerateLocalTestData.ROWS_ROWCOLVAL_DATASET,
                GenerateLocalTestData.COLS_ROWCOLVAL_DATASET,
                RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD)
                //Matrix.Format.SPARSE_MATRIX,
                //Matrix.Layout.ROW_LAYOUT
        );
    }

    private boolean isOwnPartition(int row, int col, int numNodes) {
        double numOfRowsPerInstance = (double) GenerateLocalTestData.ROWS_ROWCOLVAL_DATASET / numNodes;
        double partition = row / numOfRowsPerInstance;
        return Utils.toInt((long) (partition % numNodes)) == instanceContext.jobContext.instanceID;
    }

    @Override
    public void compute() {
        final Matrix matrix = dataManager.getObject(FILE);
        Matrix.PartitionShape shape = new MatrixByRowPartitioner(
                instanceContext.jobContext.instanceID,
                NUM_NODES,
                GenerateLocalTestData.ROWS_ROWCOLVAL_DATASET,
                GenerateLocalTestData.COLS_ROWCOLVAL_DATASET
        ).getPartitionShape();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(FILE));
            String line = null;
            while((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                int row = Integer.parseInt(parts[0]);
                int col = Integer.parseInt(parts[1]);
                double val = Double.parseDouble(parts[2]);
                if(isOwnPartition(row, col, NUM_NODES)) {
                    double matrixVal = matrix.get(row % shape.getRows(), col % shape.getCols());
                    if(matrixVal != val) {
                        //throw new RuntimeException(instanceContext.instanceID + ": matrix("+row+","+col+") is "+matrixVal+" but should be "+val);
                        System.out.println(instanceContext.jobContext.instanceID + ": matrix("+row+" % "+shape.getRows()+","+col+" % "+shape.getCols()+") is "+matrixVal+" but should be "+val);
                    }
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

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        PrintStream old = System.out;
        System.setOut(ps);

        PServerExecutor.LOCAL
                .run(ThreadedMatrixLoaderTestJob.class, 1)
                .results(res)
                .done();

        System.out.flush();
        String out = baos.toString();
        System.setOut(old);

        System.out.println(out);
    }
}