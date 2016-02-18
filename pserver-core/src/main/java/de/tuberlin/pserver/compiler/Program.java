package de.tuberlin.pserver.compiler;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.unit.controlflow.base.Body;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.core.events.EventDispatcher;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.types.typeinfo.DistributedTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;

public abstract class Program extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected static final Logger LOG = LoggerFactory.getLogger(Program.class);

    protected RuntimeManager runtimeManager;

    private Lifecycle lifecycle;

    public ProgramContext programContext;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public Program() { super(true); }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public Lifecycle getLifecycle() { return lifecycle; }

    public void injectContext(final ProgramContext programContext) throws Exception {

        this.programContext = Preconditions.checkNotNull(programContext);

        this.runtimeManager = programContext.runtimeContext.runtimeManager;

        this.lifecycle = new Lifecycle(programContext);
    }

    public void result(final Serializable... obj) {
        programContext.setResults(Arrays.asList(obj));
    }

    public void run() throws Exception {

        final String slotIDStr = "[" + programContext.nodeID + "]";

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

    // ---------------------------------------------------

    public static DistributedTypeInfo[] state(DistributedTypeInfo... objects) { return objects; }

    public static void atomic(DistributedTypeInfo[] stateObjects, final Body body) throws Exception {

        for (final DistributedTypeInfo stateObj :stateObjects)
            stateObj.lock();

        body.body();

        for (final DistributedTypeInfo stateObj :stateObjects)
            stateObj.unlock();
    }
}
