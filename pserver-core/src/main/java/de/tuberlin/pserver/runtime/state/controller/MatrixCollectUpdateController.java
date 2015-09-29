package de.tuberlin.pserver.runtime.state.controller;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.types.DistributedMatrix;

public class MatrixCollectUpdateController extends RemoteUpdateController {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final DistributedMatrix matrix;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MatrixCollectUpdateController(final SlotContext slotContext,
                                         final String stateName,
                                         final DistributedMatrix matrix) {
        super(slotContext, stateName);

        // Preconditions.checkState(matrix.completeMatrix);
        // instead check if numRowPartitions == 1 && numColPartitions == 1

        this.matrix = Preconditions.checkNotNull(matrix);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void publishUpdate(SlotContext sc) throws Exception {

    }

    @Override
    public void pullUpdate(SlotContext sc) throws Exception {
        matrix.collectRemotePartitions();
    }
}
