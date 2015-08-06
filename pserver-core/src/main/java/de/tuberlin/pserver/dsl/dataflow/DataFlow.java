package de.tuberlin.pserver.dsl.dataflow;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.runtime.InstanceContext;
import de.tuberlin.pserver.runtime.dht.DHTKey;


public final class DataFlow {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private InstanceContext instanceContext;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DataFlow(final InstanceContext instanceContext) {
        this.instanceContext = Preconditions.checkNotNull(instanceContext);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public <T extends SharedObject> DHTKey put(final String name, final T obj) {
        return instanceContext.jobContext.dataManager.putObject(name, obj);
    }

    public <T extends SharedObject> T get(final String name) {
        return instanceContext.jobContext.dataManager.getObject(name);
    }
}
