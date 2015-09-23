package de.tuberlin.pserver.runtime;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.dsl.controlflow.ControlFlow;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.dsl.dataflow.DataFlow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;

public abstract class MLProgram extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected static final Logger LOG = LoggerFactory.getLogger(MLProgram.class);

    protected ExecutionManager executionManager;

    protected DataManager dataManager;

    public SlotContext slotContext;

    private Program program;

    private MLProgramLinker programLinker;

    // ---------------------------------------------------

    public ControlFlow CF;

    public DataFlow DF;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MLProgram() {
        super(true);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void injectContext(final SlotContext slotContext) throws Exception {

        this.slotContext = Preconditions.checkNotNull(slotContext);

        this.executionManager = slotContext.runtimeContext.executionManager;

        this.dataManager = slotContext.runtimeContext.dataManager;

        this.program = new Program(slotContext);

        this.CF = slotContext.CF;

        this.DF = slotContext.DF;

        programLinker = new MLProgramLinker(slotContext.programContext, getClass());
    }

    public void result(final Serializable... obj) {
        if (slotContext.slotID == 0) {
            dataManager.setResults(slotContext.programContext.programID, Arrays.asList(obj));
        }
    }

    // ---------------------------------------------------
    // Lifecycle.
    // ---------------------------------------------------

    public void define(final Program program) {}

    // ---------------------------------------------------
    // Lifecycle Execution.
    // ---------------------------------------------------

    public void run() throws Exception {

        define(program);

        final String slotIDStr = "[" + slotContext.runtimeContext.nodeID
                + " | " + slotContext.slotID + "] ";

        program.enter();

            programLinker.link(slotContext, this);

            programLinker.defineUnits(this, program);

            programLinker.fetchStateObjects(this);



            {
                LOG.info(slotIDStr + "Enter " + program.slotContext.programContext.simpleClassName + " pre-process phase.");

                final long start = System.currentTimeMillis();

                if (program.preProcessPhase != null) program.preProcessPhase.body();

                final long end = System.currentTimeMillis();

                LOG.info(slotIDStr + "Leave " + program.slotContext.programContext.simpleClassName
                        + " pre-process phase [duration: " + (end - start) + " ms].");
            }

            {
                LOG.info(slotIDStr + "Enter " + program.slotContext.programContext.simpleClassName + " process phase.");

                final long start = System.currentTimeMillis();

                if (program.processPhase != null) program.processPhase.body();

                final long end = System.currentTimeMillis();

                LOG.info(slotIDStr + "Leave " + program.slotContext.programContext.simpleClassName +
                        " process phase [duration: " + (end - start) + " ms].");
            }

            {
                LOG.info(slotIDStr + "Enter " + program.slotContext.programContext.simpleClassName + " post-process phase.");

                final long start = System.currentTimeMillis();

                if (program.postProcessPhase != null) program.postProcessPhase.body();

                final long end = System.currentTimeMillis();

                LOG.info(slotIDStr + "Leave " + program.slotContext.programContext.simpleClassName
                        + " post-process phase [duration: " + (end - start) + " ms].");
            }

        program.leave();
    }
}
