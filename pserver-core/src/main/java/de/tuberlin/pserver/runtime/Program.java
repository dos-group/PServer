package de.tuberlin.pserver.runtime;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;

public abstract class Program extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected static final Logger LOG = LoggerFactory.getLogger(Program.class);

    protected DataManager dataManager;

    private Lifecycle lifecycle;

    private ProgramCompiler programCompiler;

    public ProgramContext programContext;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public Program() { super(true); }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void injectContext(final ProgramContext programContext) throws Exception {

        this.programContext = Preconditions.checkNotNull(programContext);

        this.dataManager = programContext.runtimeContext.dataManager;

        this.lifecycle = new Lifecycle(programContext);

        this.programCompiler = new ProgramCompiler(programContext, getClass());
    }

    public void result(final Serializable... obj) {
        dataManager.setResults(programContext.programID, Arrays.asList(obj));
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void run() throws Exception {

        final String slotIDStr = "[" + programContext.runtimeContext.nodeID + "]";

        programCompiler.link(this);

        programCompiler.defineUnits(this, lifecycle);

        programCompiler.fetchStateObjects(this);

        {
            LOG.info(slotIDStr + "Enter " + lifecycle.programContext.simpleClassName + " pre-process phase.");

            final long start = System.currentTimeMillis();

            if (lifecycle.preProcessPhase != null) lifecycle.preProcessPhase.body();

            final long end = System.currentTimeMillis();

            LOG.info(slotIDStr + "Leave " + lifecycle.programContext.simpleClassName
                    + " pre-process phase [duration: " + (end - start) + " ms].");
        }

        {
            LOG.info(slotIDStr + "Enter " + lifecycle.programContext.simpleClassName + " process phase.");

            final long start = System.currentTimeMillis();

            if (lifecycle.processPhase != null) lifecycle.processPhase.body();

            final long end = System.currentTimeMillis();

            LOG.info(slotIDStr + "Leave " + lifecycle.programContext.simpleClassName +
                    " process phase [duration: " + (end - start) + " ms].");
        }

        {
            LOG.info(slotIDStr + "Enter " + lifecycle.programContext.simpleClassName + " post-process phase.");

            final long start = System.currentTimeMillis();

            if (lifecycle.postProcessPhase != null) lifecycle.postProcessPhase.body();

            final long end = System.currentTimeMillis();

            LOG.info(slotIDStr + "Leave " + lifecycle.programContext.simpleClassName
                    + " post-process phase [duration: " + (end - start) + " ms].");
        }
    }
}
