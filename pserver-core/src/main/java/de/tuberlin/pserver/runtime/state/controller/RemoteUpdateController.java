package de.tuberlin.pserver.runtime.state.controller;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.compiler.ProgramContext;
import de.tuberlin.pserver.runtime.state.filter.UpdateFilter;
import de.tuberlin.pserver.runtime.state.merger.UpdateMerger;

public abstract class RemoteUpdateController {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final ProgramContext programContext;

    protected final String stateName;

    protected UpdateMerger merger;

    protected UpdateFilter filter;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public RemoteUpdateController(final ProgramContext programContext, final String stateName){
        this.programContext  = Preconditions.checkNotNull(programContext);
        this.stateName       = Preconditions.checkNotNull(stateName);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void setUpdateMerger(final UpdateMerger merger) { this.merger = merger; }

    public void setUpdateFilter(final UpdateFilter filter) { this.filter = filter; }

    // ---------------------------------------------------

    public abstract void publishUpdate() throws Exception;

    public abstract void pullUpdate() throws Exception;
}