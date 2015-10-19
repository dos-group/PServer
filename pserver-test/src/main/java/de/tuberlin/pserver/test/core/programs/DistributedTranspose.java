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

    private static final long ROWS = 4;
    private static final long COLS = 4;

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

            matrix.transpose();

        });
    }

    public static void main(String[] args) {
        System.setProperty("simulation.numNodes", "4");
        PServerExecutor.LOCAL
                .run(DistributedTranspose.class)
                .done();
    }
}
