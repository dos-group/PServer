package de.tuberlin.pserver.dsl.dataflow;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.delta.MatrixDelta;
import de.tuberlin.pserver.runtime.delta.MatrixDeltaManager;
import de.tuberlin.pserver.runtime.dht.DHTKey;
import de.tuberlin.pserver.runtime.dht.DHTObject;
import de.tuberlin.pserver.runtime.dht.types.EmbeddedDHTObject;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public final class DataFlow {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private SlotContext slotContext;

    private DataManager dataManager;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DataFlow(final SlotContext slotContext) {
        this.slotContext = Preconditions.checkNotNull(slotContext);
        this.dataManager = slotContext.programContext.runtimeContext.dataManager;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public <T extends SharedObject> DHTKey put(final String name, final T obj) {
        return slotContext.programContext.runtimeContext.dataManager.putObject(name, obj);
    }

    public <T extends SharedObject> T get(final String name) {
        return slotContext.programContext.runtimeContext.dataManager.getObject(name);
    }

    // ---------------------------------------------------

    public void computeDelta(final String objNames) throws Exception {
        Preconditions.checkNotNull(objNames);
        final StringTokenizer st = new StringTokenizer(objNames, ",");
        while (st.hasMoreTokens()) {
            final String name = st.nextToken().replaceAll("\\s+", "");
            final MatrixDeltaManager deltaManager =
                    (MatrixDeltaManager) slotContext.programContext.get(name + "-Delta-Manager");
            deltaManager.extractDeltas(slotContext);
        }
    }

    // ---------------------------------------------------

    public void pull(final String objNames) throws Exception {
        Preconditions.checkNotNull(objNames);
        final StringTokenizer st = new StringTokenizer(objNames, ",");
        while (st.hasMoreTokens()) {
            final String name = st.nextToken().replaceAll("\\s+", "");
            final MatrixDeltaManager deltaManager =
                    (MatrixDeltaManager) slotContext.programContext.get(name + "-Delta-Manager");
            slotContext.CF.select().slot(0).exe(() -> {
                final List<MatrixDelta> remoteDeltas = new ArrayList<>();
                final DHTObject[] dhtObjects = dataManager.pullFrom(name + "-Delta", dataManager.remoteNodeIDs);
                if (dhtObjects.length > 0) {
                    for (final DHTObject obj : dhtObjects) {
                        if (((EmbeddedDHTObject) obj).object instanceof MatrixDelta) {
                            remoteDeltas.add((MatrixDelta) ((EmbeddedDHTObject) obj).object);
                        } else
                            throw new IllegalStateException("" + ((EmbeddedDHTObject) obj).object);
                    }
                } else
                    throw new IllegalStateException();
                deltaManager.setRemoteDeltas(remoteDeltas);
            });
            slotContext.CF.syncSlots();
            deltaManager.integrateDelta(slotContext);
        }
    }
}
