package de.tuberlin.pserver.app;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;

public abstract class PServerJob {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected static final Logger LOG = LoggerFactory.getLogger(PServerJob.class);

    protected PServerContext ctx;

    protected DataManager dataManager;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void injectContext(final PServerContext ctx) {
        this.ctx = Preconditions.checkNotNull(ctx);
        this.dataManager = ctx.dataManager;
    }

    public PServerContext getJobContext() { return ctx; }

    public void result(final Serializable... obj) {
        if (ctx.threadID == 0) {
            dataManager.setResults(ctx.jobUID, Arrays.asList(obj));
        }
    }

    // ---------------------------------------------------
    // Job Lifecycle.
    // ---------------------------------------------------

    public void prologue() {}

    public abstract void compute();

    public void epilogue() {}
}
