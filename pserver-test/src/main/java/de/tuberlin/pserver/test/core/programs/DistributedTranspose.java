package de.tuberlin.pserver.test.core.programs;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.utils.Utils;
import de.tuberlin.pserver.runtime.filesystem.record.RowColValRecordIteratorProducer;
import de.tuberlin.pserver.runtime.partitioning.MatrixByRowPartitioner;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MutableMatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.ReusableMatrixEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DistributedTranspose extends Program {

    private static final long ROWS = 2;
    private static final long COLS = 8;
    private static final int NUM_NODES = 2;

    private static final double[] VALUES = new double[] {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};

    // use this, if you want to run this test directly outside the IntegrationTestSuite
  private final String FILE = "pserver-test/src/main/resources/distributed_transpose_matrix.csv";

//    private final String FILE = "src/main/resources/distributed_transpose_matrix.csv";

    private static final Logger LOG = LoggerFactory.getLogger(DistributedTranspose.class);

    @State(
            path = FILE,
            rows = ROWS,
            cols = COLS,
            scope = Scope.PARTITIONED
    )
    public Matrix matrix;

    @Unit
    public void main(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            Matrix transposed = matrix.transpose();

            Preconditions.checkState(transposed.rows() == COLS);
            Preconditions.checkState(transposed.cols() == ROWS);

            System.out.println(transposed.toString());

            //MatrixByRowPartitioner partitioner = new MatrixByRowPartitioner(COLS, ROWS, this.programContext.runtimeContext.nodeID, NUM_NODES);
            //Matrix.PartitionShape shape = partitioner.getPartitionShape();

//            for(long row = shape.rowOffset; row < shape.rowOffset + shape.rows; row++) {
//                for(long col = shape.colOffset; col < shape.colOffset + shape.cols; col++) {
//                    double expectedValue = VALUES[Utils.getPos(row, col, Layout.COLUMN_LAYOUT, ROWS, COLS)];
//                    double actualValue = transposed.get(row, col);
//                    Preconditions.checkState(expectedValue == actualValue, "("+row+","+col+"): exptected: " + expectedValue + " actual: " + actualValue);
//                    System.out.println("("+row+","+col+"): ok");
//                }
//            }

        });
    }

    public static void main(String[] args) {
        System.setProperty("simulation.numNodes", String.valueOf(NUM_NODES));
        PServerExecutor.LOCAL
                .run(DistributedTranspose.class)
                .done();
    }
}
