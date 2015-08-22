package de.tuberlin.pserver.runtime.state.controller;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.state.filter.UpdateFilter;
import de.tuberlin.pserver.runtime.state.merger.UpdateMerger;

public abstract class RemoteUpdateController {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final SlotContext slotContext;

    protected final String stateName;

    protected UpdateMerger merger;

    protected UpdateFilter filter;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public RemoteUpdateController(final SlotContext slotContext, final String stateName){
        this.slotContext  = Preconditions.checkNotNull(slotContext);
        this.stateName    = Preconditions.checkNotNull(stateName);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void setUpdateMerger(final UpdateMerger merger) { this.merger = merger; }

    public void setUpdateFilter(final UpdateFilter filter) { this.filter = filter; }

    // ---------------------------------------------------

    public abstract void publishUpdate(final SlotContext sc) throws Exception;

    public abstract void pullUpdate(final SlotContext sc) throws Exception;
}