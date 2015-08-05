package de.tuberlin.pserver.dsl.cf;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.InstanceContext;

public final class ControlFlow {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private InstanceContext instanceContext;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public ControlFlow(final InstanceContext instanceContext) {
        this.instanceContext = Preconditions.checkNotNull(instanceContext);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public int numNodes() { return instanceContext.jobContext.numOfNodes; }

    public int numInstances() { return instanceContext.jobContext.numOfInstances; }

    public Iteration iterate() { return new Iteration(instanceContext); }

    public Selection select() { return new Selection(instanceContext); }
}
