package de.tuberlin.pserver.runtime.events;


import com.google.gson.Gson;
import de.tuberlin.pserver.commons.json.GsonUtils;
import de.tuberlin.pserver.runtime.core.network.MachineDescriptor;
import de.tuberlin.pserver.runtime.core.network.NetEvent;

import java.util.UUID;

public final class ProgramFailureEvent extends NetEvent {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String PSERVER_FAILURE_JOB_EVENT = "PROGRAM_FAILURE_JOB_EVENT";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private transient static Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

    private static final long serialVersionUID = -1L;

    public final MachineDescriptor machine;

    public final UUID programID;

    public final int nodeID;

    public final int slotID;

    public final String programName;

    @GsonUtils.Exclude
    public final String stackTrace;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ProgramFailureEvent() { this(null, null, -1, -1, null, null); }
    public ProgramFailureEvent(final MachineDescriptor machine,
                               final UUID programID,
                               final int nodeID,
                               final int slotID,
                               final String programName,
                               final String exceptionMessage) {

        super(PSERVER_FAILURE_JOB_EVENT);

        this.machine        = machine;
        this.programID      = programID;
        this.nodeID         = nodeID;
        this.slotID         = slotID;
        this.programName    = programName;
        this.stackTrace     = exceptionMessage;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public String toString() {
        return "\nProgramFailureEvent " + gson.toJson(this) + "\n ==> JAVA STACK TRACE [\n" + stackTrace + "]";
    }
}
