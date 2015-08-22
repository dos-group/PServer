package de.tuberlin.pserver.runtime.events;


import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import de.tuberlin.pserver.commons.json.GsonUtils;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetEvents;

import java.util.UUID;

public final class ProgramFailureEvent extends NetEvents.NetEvent {

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

    public ProgramFailureEvent(final MachineDescriptor machine,
                               final UUID programID,
                               final int nodeID,
                               final int slotID,
                               final String programName,
                               final String exceptionMessage) {

        super(PSERVER_FAILURE_JOB_EVENT);

        this.machine        = Preconditions.checkNotNull(machine);
        this.programID      = Preconditions.checkNotNull(programID);
        this.nodeID         = nodeID;
        this.slotID         = slotID;
        this.programName    = Preconditions.checkNotNull(programName);
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
