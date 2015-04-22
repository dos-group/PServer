package de.tuberlin.pserver.app;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public void prologue() {}

    public abstract void compute();

    public void epilogue() {}
}
