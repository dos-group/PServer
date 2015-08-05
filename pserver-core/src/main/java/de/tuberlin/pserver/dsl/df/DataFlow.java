package de.tuberlin.pserver.dsl.df;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.InstanceContext;
import de.tuberlin.pserver.app.dht.Key;
import de.tuberlin.pserver.math.MObject;


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

    public <T extends MObject> Key put(final String name, final T obj) {
        return instanceContext.jobContext.dataManager.putObject(name, obj);
    }

    public <T extends MObject> T get(final String name) {
        return instanceContext.jobContext.dataManager.getObject(name);
    }
}
