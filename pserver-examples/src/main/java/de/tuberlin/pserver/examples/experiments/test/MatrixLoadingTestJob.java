package de.tuberlin.pserver.examples.experiments.test;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.dsl.state.GlobalScope;
import de.tuberlin.pserver.dsl.state.SharedState;
import de.tuberlin.pserver.examples.experiments.sgd.GenerateLocalTestData;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.utils.Utils;
import de.tuberlin.pserver.runtime.MLProgram;
import de.tuberlin.pserver.runtime.filesystem.record.IRecordFactory;
import de.tuberlin.pserver.runtime.partitioning.MatrixByRowPartitioner;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.ImmutableMatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MutableMatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.ReusableMatrixEntry;

import java.io.*;
import java.util.List;

public class MatrixLoadingTestJob extends MLProgram  {

    private static final int NUM_NODES = 7;
    private static final long ROWS = 10000;
    private static final long COLS = 2500;
    private static final String FILE = "datasets/rowcolval_dataset_"+ROWS+"_"+COLS+".csv";

    @SharedState(
            globalScope = GlobalScope.PARTITIONED,
            rows = ROWS,
            cols = COLS,
            path = FILE
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

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<List<Serializable>> res = Lists.newArrayList();

        System.setProperty("simulation.numNodes", String.valueOf(NUM_NODES));

        PServerExecutor.LOCAL
                .run(MatrixLoadingTestJob.class, 1)
                .results(res)
                .done();
    }
}
