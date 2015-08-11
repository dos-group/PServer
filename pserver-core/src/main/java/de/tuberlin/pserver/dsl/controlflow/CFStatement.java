package de.tuberlin.pserver.dsl.controlflow;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import de.tuberlin.pserver.commons.json.GsonUtils;
import de.tuberlin.pserver.runtime.SlotContext;

import java.util.UUID;

public abstract class CFStatement {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static class ProfilingData {

        private transient static Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

        public final long duration;

        public ProfilingData(final long duration) { this.duration = duration; }

        @Override public String toString() { return gson.toJson(this); }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final UUID id  = UUID.randomUUID();

    public final SlotContext slotContext;

    private ProfilingData profilingData;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public CFStatement(final SlotContext slotContext) {
        this.slotContext = Preconditions.checkNotNull(slotContext);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void enter() { slotContext.jobContext.executionManager.pushFrame(this); }

    public void leave() { slotContext.jobContext.executionManager.popFrame(); }

    // ---------------------------------------------------

    public ProfilingData getProfilingData() { return profilingData; }

    public void printProfilingData() { System.out.println(profilingData.toString()); }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    protected void setProfilingData(final ProfilingData profilingData) {
        this.profilingData = Preconditions.checkNotNull(profilingData);
    }
}
