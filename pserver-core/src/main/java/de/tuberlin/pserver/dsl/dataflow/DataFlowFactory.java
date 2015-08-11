package de.tuberlin.pserver.dsl.dataflow;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.dht.DHTKey;


public final class DataFlowFactory {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private SlotContext slotContext;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DataFlowFactory(final SlotContext slotContext) {
        this.slotContext = Preconditions.checkNotNull(slotContext);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public <T extends SharedObject> DHTKey put(final String name, final T obj) {
        return slotContext.jobContext.dataManager.putObject(name, obj);
    }

    public <T extends SharedObject> T get(final String name) {
        return slotContext.jobContext.dataManager.getObject(name);
    }
}
