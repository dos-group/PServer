package de.tuberlin.pserver.runtime.events;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import de.tuberlin.pserver.commons.json.GsonUtils;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetEvents;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public final class PServerJobResultEvent extends NetEvents.NetEvent {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String PSERVER_JOB_RESULT_EVENT = "PSERVER_JOB_RESULT_EVENT";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private transient static Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

    private static final long serialVersionUID = -1L;

    public final MachineDescriptor workerMachine;

    public final int nodeID;

    public final UUID jobUID;

    public final List<Serializable> resultObjects;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PServerJobResultEvent(final MachineDescriptor workerMachine,
                                 final int nodeID,
                                 final UUID jobUID,
                                 final List<Serializable> resultObjects) {

        super(PSERVER_JOB_RESULT_EVENT);

        this.workerMachine  = Preconditions.checkNotNull(workerMachine);
        this.nodeID         = nodeID;
        this.jobUID         = Preconditions.checkNotNull(jobUID);
        this.resultObjects  = resultObjects != null ? Collections.unmodifiableList(resultObjects) : null;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public String toString() { return "\nPServerJobSubmissionEvent " + gson.toJson(this); }
}