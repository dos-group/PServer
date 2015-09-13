package de.tuberlin.pserver.runtime.state.controller;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.dht.DHTObject;
import de.tuberlin.pserver.runtime.dht.types.EmbeddedDHTObject;
import de.tuberlin.pserver.runtime.state.merger.VectorUpdateMerger;

import java.util.ArrayList;
import java.util.List;

public class VectorMergeUpdateController extends RemoteUpdateController {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Vector stateVector;

    private EmbeddedDHTObject<Vector> shadowVector;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public VectorMergeUpdateController(final SlotContext slotContext,
                                       final String stateName,
                                       final Vector vector) {
        super(slotContext, stateName);

        this.stateVector = Preconditions.checkNotNull(vector);

        this.shadowVector = new EmbeddedDHTObject<>(Preconditions.checkNotNull(vector).copy());

        slotContext.runtimeContext.dataManager.putObject(stateName + "-Shadow", shadowVector);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void publishUpdate(final SlotContext sc) throws Exception {

        sc.CF.syncSlots();

        sc.CF.parScope().slot(0).exe( () -> {

            synchronized (shadowVector.lock) {

                shadowVector.object.assign(stateVector);
            }
        });

        sc.CF.syncSlots();
    }

    @Override
    public void pullUpdate(final SlotContext sc) throws Exception {

        Preconditions.checkState(merger != null);

        Preconditions.checkState(merger instanceof VectorUpdateMerger);

        final VectorUpdateMerger vectorUpdateMerger = (VectorUpdateMerger)merger;

        sc.CF.syncSlots();

        sc.CF.parScope().slot(0).exe( () -> {

            final DataManager dataManager = sc.runtimeContext.dataManager;

            final DHTObject[] dhtObjects = dataManager.pullFrom(stateName + "-Shadow", dataManager.remoteNodeIDs);

            Preconditions.checkState(dhtObjects.length > 0);

            final List<Vector> remoteVectors = new ArrayList<>();

            for (final DHTObject obj : dhtObjects) {

                Preconditions.checkState(((EmbeddedDHTObject) obj).object instanceof Vector);

                final Vector remoteVector = (Vector)((EmbeddedDHTObject) obj).object;

                remoteVectors.add(remoteVector);
            }

            sc.programContext.put(stateName + "-Remote-Vector-List", remoteVectors);
        });

        sc.CF.syncSlots();

        @SuppressWarnings("unchecked")
        final List<Vector> remoteVectors = sc.programContext.get(stateName + "-Remote-Vector-List");

        for (final Vector remoteVector : remoteVectors) {

            sc.CF.loop().parExe(remoteVector, (e, it) -> {

                final long i = it.getCurrentElementNum();

                vectorUpdateMerger.mergeElement(i, stateVector.get(i),remoteVector.get(i));
            });
        }

        sc.CF.syncSlots();

        sc.CF.parScope().slot(0).exe(() -> sc.programContext.delete(stateName + "-Remote-Vector-List"));
    }
}