package de.tuberlin.pserver.test.core.programs;

/*import com.google.common.base.Preconditions;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.MatrixFormat;
import de.tuberlin.pserver.math.matrix.Matrix64F;
import de.tuberlin.pserver.runtime.filesystem.recordold.RowColValRecordIteratorProducer;
import de.tuberlin.pserver.runtime.state.mtxentries.MutableMatrixEntry;
import de.tuberlin.pserver.runtime.state.mtxentries.ReusableMatrixEntry;
import de.tuberlin.pserver.runtime.state.partitioner.RowPartitioner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class MatrixDenseLoadingRowColValTestJob extends Program {

    private static final long ROWS = 1000;
    private static final long COLS = 250;

    // use this, if you want to run this test directly outside the IntegrationTestSuite
    // private final String FILE = "pserver-test/src/main/resources/rowcolval_dataset_1000_250_shuffeled.csv";

    private final String FILE = "pserver-test/src/main/resources/rowcolval_dataset_1000_250_shuffeled.csv";

    @State(
            path = FILE,
            rows = ROWS,
            cols = COLS,
            scope = Scope.PARTITIONED,
            recordFormat = RowColValRecordIteratorProducer.class,
            matrixFormat = MatrixFormat.DENSE_FORMAT
    )
    public Matrix64F matrix;

    @Unit
    public void main(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            matrix = runtimeManager.getDHT("matrix");
            int nodeId = programContext.nodeID;
            int numNodes = programContext.nodeDOP;
            RowPartitioner partitioner = new RowPartitioner(ROWS, COLS, nodeId, numNodes);
            ReusableMatrixEntry<Double> entry = new MutableMatrixEntry<>(-1, -1, Double.NaN);
            BufferedReader br = null;

            try {
                br = new BufferedReader(new FileReader(FILE));
                String line = null;
                while ((line = br.readLine()) != null) {

                    String[] parts = line.split(",");
                    int row = Integer.parseInt(parts[0]);
                    int col = Integer.parseInt(parts[1]);
                    double val = Double.parseDouble(parts[2]);

                    if (partitioner.getPartitionOfEntry(entry.set(row, col, Double.NaN)) == nodeId) {
                        double matrixVal = matrix.get(row, col);
                        if (matrixVal != val) {
                            System.out.println(nodeId + ": matrix(" + row + "," + col + ") is " + matrixVal + " but should be " + val);
                        }
                        Preconditions.checkState(matrixVal == val);
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
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
                .run(MatrixDenseLoadingRowColValTestJob.class)
                .done();
    }
}*/
