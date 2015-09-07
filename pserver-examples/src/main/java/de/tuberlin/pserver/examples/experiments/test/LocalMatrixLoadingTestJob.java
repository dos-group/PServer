package de.tuberlin.pserver.examples.experiments.test;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.dsl.state.GlobalScope;
import de.tuberlin.pserver.dsl.state.SharedState;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.MLProgram;
import de.tuberlin.pserver.runtime.partitioning.MatrixByRowPartitioner;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MutableMatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.ReusableMatrixEntry;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by fsander on 07.09.15.
 */
public class LocalMatrixLoadingTestJob extends MLProgram {

    private static final long ROWS = 10000;
    private static final long COLS = 2500;

    // loaded by pserver
    private static final String PSERVER_FILE = "datasets/rowcolval_dataset_" + ROWS + "_" + COLS + "_shuffeled.csv";
    // loaded by job
    private static final String PROGRAM_FILE = "datasets/rowcolval_dataset_" + ROWS + "_" + COLS + "_shuffeled.csv";

    @SharedState(
            globalScope = GlobalScope.PARTITIONED,
            rows = ROWS,
            cols = COLS,
            path = PSERVER_FILE,
            format = Format.SPARSE_FORMAT
    )
    public Matrix matrix;


    @Override
    public void define(Program program) {
        program.process(() -> {
            int nodeId = slotContext.programContext.runtimeContext.nodeID;
            int numNodes = slotContext.programContext.nodeDOP;
            MatrixByRowPartitioner partitioner = new MatrixByRowPartitioner(nodeId, numNodes, ROWS, COLS);

            ReusableMatrixEntry entry = new MutableMatrixEntry(-1, -1, Double.NaN);

            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(PROGRAM_FILE));
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
                .run(SubmitMatrixLoadingTestJob.class, 1)
                .done();
    }

}
