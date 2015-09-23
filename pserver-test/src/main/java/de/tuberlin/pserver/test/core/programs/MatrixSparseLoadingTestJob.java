package de.tuberlin.pserver.test.core.programs;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Lifecycle;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.GlobalScope;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.MLProgram;
import de.tuberlin.pserver.runtime.filesystem.record.config.RowColValRecordFormatConfig;
import de.tuberlin.pserver.runtime.partitioning.MatrixByRowPartitioner;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MutableMatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.ReusableMatrixEntry;
import de.tuberlin.pserver.types.PartitionType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class MatrixSparseLoadingTestJob extends MLProgram  {

    private static final long ROWS = 1000;
    private static final long COLS = 250;

    public Matrix matrix;

    private final String FILE;

    public MatrixSparseLoadingTestJob() {
        FILE = getClass().getClassLoader().getResource("rowcolval_dataset_1000_250_shuffeled.csv").getFile();
    }

    @Unit
    public void main(final Lifecycle lifecycle) {

        if (slotContext.slotID == 0) {
            dataManager.loadAsMatrix(
                    slotContext,
                    FILE,
                    "matrix",
                    ROWS, COLS,
                    GlobalScope.PARTITIONED,
                    PartitionType.ROW_PARTITIONED,
                    new RowColValRecordFormatConfig(),
                    Format.SPARSE_FORMAT,
                    Layout.ROW_LAYOUT
            );
        }

        lifecycle.process(() -> {

            int nodeId = slotContext.runtimeContext.nodeID;
            int numNodes = slotContext.programContext.nodeDOP;
            MatrixByRowPartitioner partitioner = new MatrixByRowPartitioner(nodeId, numNodes, ROWS, COLS);

            ReusableMatrixEntry entry = new MutableMatrixEntry(-1, -1, Double.NaN);

            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(FILE));
                String line = null;
                while((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    int row = Integer.parseInt(parts[0]);
                    int col = Integer.parseInt(parts[1]);
                    double val = Double.parseDouble(parts[2]);

                    if(partitioner.getPartitionOfEntry(entry.set(row, col, Double.NaN)) == nodeId) {
                        double matrixVal = matrix.get(row, col);
                        if(matrixVal != val) {
                            System.out.println(nodeId + ": matrix("+row+","+col+") is "+matrixVal+" but should be "+val);
                        }
                        Preconditions.checkState(matrixVal == val);
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

        });
    }

    public static void main(String[] args) {
        System.setProperty("simulation.numNodes", "4");
        PServerExecutor.LOCAL
                .run(MatrixSparseLoadingTestJob.class)
                .done();
    }

    public static class MatrixDenseLoadingTestJob extends MLProgram  {

        private static final long ROWS = 10000;
        private static final long COLS = 2500;
        private static final String FILE = "datasets/rowcolval_dataset_" + ROWS + "_" + COLS + "_shuffeled.csv";

        @State(
                globalScope = GlobalScope.PARTITIONED,
                rows = ROWS,
                cols = COLS,
                path = FILE
        )
        public Matrix matrix;


        @Override
        public void define(Lifecycle lifecycle) {
            lifecycle.process(() -> {

                int nodeId = slotContext.runtimeContext.nodeID;
                int numNodes = slotContext.programContext.nodeDOP;
                MatrixByRowPartitioner partitioner = new MatrixByRowPartitioner(nodeId, numNodes, ROWS, COLS);

                ReusableMatrixEntry entry = new MutableMatrixEntry(-1, -1, Double.NaN);

                BufferedReader br = null;
                try {
                    br = new BufferedReader(new FileReader(FILE));
                    String line = null;
                    while((line = br.readLine()) != null) {
                        String[] parts = line.split(",");
                        int row = Integer.parseInt(parts[0]);
                        int col = Integer.parseInt(parts[1]);
                        double val = Double.parseDouble(parts[2]);

                        if(partitioner.getPartitionOfEntry(entry.set(row, col, Double.NaN)) == nodeId) {
                            double matrixVal = matrix.get(row, col);
                            if(matrixVal != val) {
                                System.out.println(nodeId + ": matrix("+row+","+col+") is "+matrixVal+" but should be "+val);
                            }
                            Preconditions.checkState(matrixVal == val);
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

            });
        }

        public static void main(String[] args) {
            System.setProperty("simulation.numNodes", "4");
            PServerExecutor.LOCAL
                    .run(MatrixDenseLoadingTestJob.class)
                    .done();
        }
    }
}
