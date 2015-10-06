package de.tuberlin.pserver.examples.experiments.test;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.partitioning.MatrixByRowPartitioner;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MutableMatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.ReusableMatrixEntry;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class SubmitMatrixLoadingTestJob extends Program {

    private static final long ROWS = 10000;
    private static final long COLS = 2500;

    // loaded by pserver
    private static final String PSERVER_FILE = "hdfs://wally101.cit.tu-berlin.de:45010/rowcolval_dataset_" + ROWS + "_" + COLS + "_shuffeled.csv";
    //private static final String PSERVER_FILE = "/home/fridtjof.sander/rowcolval_dataset_" + ROWS + "_" + COLS + "_shuffeled.csv";
    // loaded by job
    private static final String PROGRAM_FILE = "/home/fridtjof.sander/rowcolval_dataset_" + ROWS + "_" + COLS + "_shuffeled.csv";

    @State(
            scope = Scope.PARTITIONED,
            rows = ROWS,
            cols = COLS,
            path = PSERVER_FILE,
            format = Format.SPARSE_FORMAT
    )
    public Matrix matrix;


    @Unit
    public void define(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            int nodeId = programContext.runtimeContext.nodeID;
            int numNodes = programContext.nodeDOP;
            MatrixByRowPartitioner partitioner = new MatrixByRowPartitioner(ROWS, COLS, nodeId, numNodes);

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
        System.setProperty("pserver.profile", "wally");
        PServerExecutor.DISTRIBUTED
                .run(SubmitMatrixLoadingTestJob.class)
                .done();
    }
}

