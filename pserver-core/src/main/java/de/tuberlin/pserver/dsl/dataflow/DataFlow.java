package de.tuberlin.pserver.dsl.dataflow;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.MLProgramLinker;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.dht.DHTKey;
import de.tuberlin.pserver.runtime.state.controller.RemoteUpdateController;

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
        return dataManager.putObject(name, obj);
    }

    public <T extends SharedObject> T get(final String name) {
        return dataManager.getObject(name);
    }

    public void publishUpdate(final String objNames) throws Exception {
        Preconditions.checkNotNull(objNames);
        final StringTokenizer st = new StringTokenizer(objNames, ",");
        while (st.hasMoreTokens()) {
            final String name = st.nextToken().replaceAll("\\s+", "");
            final RemoteUpdateController remoteUpdateController =
                    (RemoteUpdateController) slotContext.programContext.get(MLProgramLinker.remoteUpdateControllerName(name));
            remoteUpdateController.publishUpdate(slotContext);
        }
    }

    public void pullUpdate(final String objNames) throws Exception {
        Preconditions.checkNotNull(objNames);
        final StringTokenizer st = new StringTokenizer(objNames, ",");
        while (st.hasMoreTokens()) {
            final String name = st.nextToken().replaceAll("\\s+", "");
            final RemoteUpdateController remoteUpdateController =
                    (RemoteUpdateController) slotContext.programContext.get(MLProgramLinker.remoteUpdateControllerName(name));
            remoteUpdateController.pullUpdate(slotContext);
        }
    }
}
