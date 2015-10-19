package de.tuberlin.pserver.test.core.programs;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Format;
import de.tuberlin.pserver.math.matrix.Layout;
import de.tuberlin.pserver.runtime.filesystem.record.RowColValRecordIteratorProducer;
import de.tuberlin.pserver.runtime.partitioning.partitioner.RowPartitioner;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MutableMatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.ReusableMatrixEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class MatrixSparseLoadingTestJob extends Program {

    private static final long ROWS = 1000;
    private static final long COLS = 250;

    // use this, if you want to run this test directly outside the IntegrationTestSuite
//  private final String FILE = "pserver-test/src/main/resources/rowcolval_dataset_1000_250_shuffeled.csv";

    private final String FILE = "src/main/resources/rowcolval_dataset_1000_250_shuffeled.csv";

    private static final Logger LOG = LoggerFactory.getLogger(MatrixSparseLoadingTestJob.class);

    /*@State(
            path = FILE,
            rows = ROWS,
            cols = COLS,
            scope = Scope.PARTITIONED,
            recordFormat = RowColValRecordIteratorProducer.class,
            format = Format.SPARSE_FORMAT,
            layout = Layout.ROW_LAYOUT
    )
    public Matrix matrix;*/

    @Unit
    public void main(final Lifecycle lifecycle) {

        /*lifecycle.process(() -> {

            int nodeId = programContext.runtimeContext.nodeID;
            int numNodes = programContext.nodeDOP;
            RowPartitioner partitioner = new RowPartitioner(ROWS, COLS, nodeId, numNodes);

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

        });*/
    }

    public static void main(String[] args) {
        System.setProperty("simulation.numNodes", "4");
        PServerExecutor.LOCAL
                .run(MatrixSparseLoadingTestJob.class)
                .done();
    }

}