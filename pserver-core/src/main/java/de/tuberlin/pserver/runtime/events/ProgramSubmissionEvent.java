package de.tuberlin.pserver.runtime.events;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import de.tuberlin.pserver.commons.json.GsonUtils;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetEvents;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public final class ProgramSubmissionEvent extends NetEvents.NetEvent {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String PSERVER_JOB_SUBMISSION_EVENT = "PROGRAM_SUBMISSION_EVENT";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private transient static Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

    private static final long serialVersionUID = -1L;

    public final MachineDescriptor clientMachine;

    public final UUID programID;

    public final String[] args;

    @GsonUtils.Exclude
    public final List<Pair<String, byte[]>> byteCode;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ProgramSubmissionEvent(final MachineDescriptor clientMachine,
                                  final UUID programID,
                                  final List<Pair<String, byte[]>> byteCode,
                                  final String[] args) {

        super(PSERVER_JOB_SUBMISSION_EVENT);

        this.clientMachine      = Preconditions.checkNotNull(clientMachine);
        this.programID          = Preconditions.checkNotNull(programID);
        this.byteCode           = Collections.unmodifiableList(Preconditions.checkNotNull(byteCode));
        this.args               = Preconditions.checkNotNull(args);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public String toString() { return "\nProgramSubmissionEvent " + gson.toJson(this); }
}
