package de.tuberlin.pserver.runtime.events;

import com.google.gson.Gson;
import de.tuberlin.pserver.commons.json.GsonUtils;
import de.tuberlin.pserver.runtime.core.network.MachineDescriptor;
import de.tuberlin.pserver.runtime.core.network.NetEvent;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public final class ProgramResultEvent extends NetEvent {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String PSERVER_JOB_RESULT_EVENT = "PROGRAM_RESULT_EVENT";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private transient static Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

    private static final long serialVersionUID = -1L;

    public final MachineDescriptor workerMachine;

    public final int nodeID;

    public final UUID programID;

    public final List<Serializable> resultObjects;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ProgramResultEvent() { this(null, -1, null, null); }
    public ProgramResultEvent(final MachineDescriptor workerMachine,
                              final int nodeID,
                              final UUID programID,
                              final List<Serializable> resultObjects) {

        super(PSERVER_JOB_RESULT_EVENT);

        this.workerMachine  = workerMachine;
        this.nodeID         = nodeID;
        this.programID      = programID;
        this.resultObjects  = resultObjects != null ? Collections.unmodifiableList(resultObjects) : null;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public String toString() { return "\nProgramResultEvent " + gson.toJson(this); }
}