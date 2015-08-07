package de.tuberlin.pserver.dsl.controlflow;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.controlflow.iteration.Iteration;
import de.tuberlin.pserver.dsl.controlflow.selection.Selection;
import de.tuberlin.pserver.runtime.InstanceContext;

public final class ControlFlowFactory {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private InstanceContext instanceContext;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public ControlFlowFactory(final InstanceContext instanceContext) {
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
