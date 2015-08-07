package de.tuberlin.pserver.app;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.dsl.cf.ControlFlow;
import de.tuberlin.pserver.dsl.df.DataFlow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;

public abstract class PServerJob extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected static final Logger LOG = LoggerFactory.getLogger(PServerJob.class);

    protected DataManager dataManager;

    protected ControlFlow CF;

    protected DataFlow DF;

    public InstanceContext instanceContext;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PServerJob() {
        super(true);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void injectContext(final InstanceContext ctx) {
        instanceContext = Preconditions.checkNotNull(ctx);
        dataManager = ctx.jobContext.dataManager;
        CF = new ControlFlow(instanceContext);
        DF = new DataFlow(instanceContext);
    }

    public void result(final Serializable... obj) {
        if (instanceContext.instanceID == 0) {
            dataManager.setResults(instanceContext.jobContext.jobUID, Arrays.asList(obj));
        }
    }

    // ---------------------------------------------------
    // Job Lifecycle.
    // ---------------------------------------------------

    public void prologue() {}

    public abstract void compute();

    public void epilogue() {}
}
