package de.tuberlin.pserver.runtime.state.controller;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.compiler.ProgramContext;
import de.tuberlin.pserver.types.DistributedMatrix;

public class MatrixCollectUpdateController extends RemoteUpdateController {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final DistributedMatrix matrix;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MatrixCollectUpdateController(final ProgramContext programContext,
                                         final String stateName,
                                         final DistributedMatrix matrix) {
        super(programContext, stateName);

        Preconditions.checkState(matrix.completeMatrix);

        this.matrix = Preconditions.checkNotNull(matrix);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void publishUpdate() throws Exception {

    }

    @Override
    public void pullUpdate() throws Exception {
        matrix.collectRemotePartitions();
    }
}
