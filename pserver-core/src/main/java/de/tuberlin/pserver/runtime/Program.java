package de.tuberlin.pserver.runtime;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.dsl.controlflow.ControlFlow;
import de.tuberlin.pserver.dsl.controlflow.program.Lifecycle;
import de.tuberlin.pserver.dsl.dataflow.DataFlow;
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

    public SlotContext slotContext;

    private Lifecycle lifecycle;

    private ProgramLinker programLinker;

    // ---------------------------------------------------

    public ControlFlow CF;

    public DataFlow DF;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public Program() {
        super(true);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void injectContext(final SlotContext slotContext) throws Exception {

        this.slotContext = Preconditions.checkNotNull(slotContext);

        this.dataManager = slotContext.runtimeContext.dataManager;

        this.lifecycle = new Lifecycle(slotContext);

        this.CF = slotContext.CF;

        this.DF = slotContext.DF;

        programLinker = new ProgramLinker(slotContext.programContext, getClass());
    }

    public void result(final Serializable... obj) {
        if (slotContext.slotID == 0) {
            dataManager.setResults(slotContext.programContext.programID, Arrays.asList(obj));
        }
    }

    // ---------------------------------------------------
    // Lifecycle.
    // ---------------------------------------------------

    public void define(final Lifecycle lifecycle) {}

    // ---------------------------------------------------
    // Lifecycle Execution.
    // ---------------------------------------------------

    public void run() throws Exception {

        define(lifecycle);

        final String slotIDStr = "[" + slotContext.runtimeContext.nodeID
                + " | " + slotContext.slotID + "] ";

        //program.enter();

        programLinker.link(slotContext, this);

        programLinker.defineUnits(this, lifecycle);

        programLinker.fetchStateObjects(this);

        {
            LOG.info(slotIDStr + "Enter " + lifecycle.slotContext.programContext.simpleClassName + " pre-process phase.");

            final long start = System.currentTimeMillis();

            if (lifecycle.preProcessPhase != null) lifecycle.preProcessPhase.body();

            final long end = System.currentTimeMillis();

            LOG.info(slotIDStr + "Leave " + lifecycle.slotContext.programContext.simpleClassName
                    + " pre-process phase [duration: " + (end - start) + " ms].");
        }

        {
            LOG.info(slotIDStr + "Enter " + lifecycle.slotContext.programContext.simpleClassName + " process phase.");

            final long start = System.currentTimeMillis();

            if (lifecycle.processPhase != null) lifecycle.processPhase.body();

            final long end = System.currentTimeMillis();

            LOG.info(slotIDStr + "Leave " + lifecycle.slotContext.programContext.simpleClassName +
                    " process phase [duration: " + (end - start) + " ms].");
        }

        {
            LOG.info(slotIDStr + "Enter " + lifecycle.slotContext.programContext.simpleClassName + " post-process phase.");

            final long start = System.currentTimeMillis();

            if (lifecycle.postProcessPhase != null) lifecycle.postProcessPhase.body();

            final long end = System.currentTimeMillis();

            LOG.info(slotIDStr + "Leave " + lifecycle.slotContext.programContext.simpleClassName
                    + " post-process phase [duration: " + (end - start) + " ms].");
        }

        //program.leave();
    }
}
