package de.tuberlin.pserver.runtime.state.controller;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.dht.DHTObject;
import de.tuberlin.pserver.runtime.dht.types.EmbeddedDHTObject;
import de.tuberlin.pserver.runtime.state.merger.MatrixUpdateMerger;

import java.util.ArrayList;
import java.util.List;

public class MatrixMergeUpdateController extends RemoteUpdateController {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Matrix stateMatrix;

    private EmbeddedDHTObject<Matrix> shadowMatrix;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MatrixMergeUpdateController(final SlotContext slotContext,
                                       final String stateName,
                                       final Matrix matrix) {
        super(slotContext, stateName);

        this.stateMatrix = Preconditions.checkNotNull(matrix);

        this.shadowMatrix = new EmbeddedDHTObject<>(Preconditions.checkNotNull(matrix).copy());

        slotContext.runtimeContext.dataManager.putObject(stateName + "-Shadow", shadowMatrix);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void publishUpdate(final SlotContext sc) throws Exception {

        sc.CF.syncSlots();

        sc.CF.parUnit(0).exe( () -> {

            synchronized (shadowMatrix.lock) {

                shadowMatrix.object.assign(stateMatrix);
            }
        });

        sc.CF.syncSlots();
    }

    @Override
    public void pullUpdate(final SlotContext sc) throws Exception {

        Preconditions.checkState(merger != null);

        Preconditions.checkState(merger instanceof MatrixUpdateMerger);

        final MatrixUpdateMerger matrixUpdateMerger = (MatrixUpdateMerger)merger;

        sc.CF.syncSlots();

        sc.CF.parUnit(0).exe( () -> {

            final DataManager dataManager = sc.runtimeContext.dataManager;

            final DHTObject[] dhtObjects = dataManager.pullFrom(stateName + "-Shadow", dataManager.remoteNodeIDs);

            Preconditions.checkState(dhtObjects.length > 0);

            final List<Matrix> remoteMatrices = new ArrayList<>();

            for (final DHTObject obj : dhtObjects) {

                Preconditions.checkState(((EmbeddedDHTObject) obj).object instanceof Matrix);

                final Matrix remoteMatrix = (Matrix)((EmbeddedDHTObject) obj).object;

                remoteMatrices.add(remoteMatrix);
            }

            sc.programContext.put(stateName + "-Remote-Matrix-List", remoteMatrices);
        });

        sc.CF.syncSlots();

        @SuppressWarnings("unchecked")
        final List<Matrix> remoteMatrices = (List<Matrix>) sc.programContext.get(stateName + "-Remote-Matrix-List");

        for (final Matrix remoteMatrix : remoteMatrices) {

            sc.CF.loop().exe(stateMatrix, (e, i, j, v) -> {

                stateMatrix.set(i, j, matrixUpdateMerger.mergeElement(i, j, v, remoteMatrix.get(i, j)));
            });
        }

        sc.CF.syncSlots();

        sc.CF.parUnit(0).exe(() -> sc.programContext.delete(stateName + "-Remote-Matrix-List"));
    }
}