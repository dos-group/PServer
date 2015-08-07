package de.tuberlin.pserver.runtime;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.dsl.controlflow.ControlFlowFactory;
import de.tuberlin.pserver.dsl.dataflow.DataFlowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;

public abstract class JobExecutable extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected static final Logger LOG = LoggerFactory.getLogger(JobExecutable.class);

    protected DataManager dataManager;

    protected ControlFlowFactory CF;

    protected DataFlowFactory DF;

    public InstanceContext instanceContext;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public JobExecutable() {
        super(true);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void injectContext(final InstanceContext ctx) {
        instanceContext = Preconditions.checkNotNull(ctx);
        dataManager = ctx.jobContext.dataManager;
        CF = new ControlFlowFactory(instanceContext);
        DF = new DataFlowFactory(instanceContext);
    }

    public void result(final Serializable... obj) {
        if (instanceContext.instanceID == 0) {
            dataManager.setResults(instanceContext.jobContext.jobUID, Arrays.asList(obj));
        }
    }

    // ---------------------------------------------------
    // Lifecycle.
    // ---------------------------------------------------

    public void prologue() {}

    public abstract void compute();

    public void epilogue() {}
}
