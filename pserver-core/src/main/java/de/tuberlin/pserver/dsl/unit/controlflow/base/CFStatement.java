package de.tuberlin.pserver.dsl.unit.controlflow.base;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import de.tuberlin.pserver.commons.json.GsonUtils;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

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

    public final ProgramContext programContext;

    private ProfilingData profilingData;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public CFStatement(final ProgramContext programContext) {
        this.programContext = Preconditions.checkNotNull(programContext);
    }

    // ---------------------------------------------------
    // Public Methods.
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
