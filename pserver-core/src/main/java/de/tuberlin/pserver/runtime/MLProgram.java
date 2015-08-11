package de.tuberlin.pserver.runtime;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.dsl.controlflow.ControlFlowFactory;
import de.tuberlin.pserver.dsl.dataflow.DataFlowFactory;
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

    protected ControlFlowFactory CF;

    protected DataFlowFactory DF;

    public SlotContext slotContext;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MLProgram() {
        super(true);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void injectContext(final SlotContext slotContext) {
        this.slotContext = Preconditions.checkNotNull(slotContext);
        executionManager = slotContext.programContext.runtimeContext.executionManager;
        dataManager = slotContext.programContext.runtimeContext.dataManager;
        CF = new ControlFlowFactory(this.slotContext);
        DF = new DataFlowFactory(this.slotContext);
    }

    public void result(final Serializable... obj) {
        if (slotContext.slotID == 0) {
            dataManager.setResults(slotContext.programContext.programID, Arrays.asList(obj));
        }
    }

    // ---------------------------------------------------
    // Lifecycle.
    // ---------------------------------------------------

    public void prologue() {}

    public abstract void compute();

    public void epilogue() {}
}
