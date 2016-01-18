package de.tuberlin.pserver.runtime.events;

import com.google.gson.Gson;
import de.tuberlin.pserver.commons.json.GsonUtils;
import de.tuberlin.pserver.math.tuples.Tuple2;
import de.tuberlin.pserver.runtime.core.network.MachineDescriptor;
import de.tuberlin.pserver.runtime.core.network.NetEvent;

import java.util.List;
import java.util.UUID;

public final class ProgramSubmissionEvent extends NetEvent {

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

    @GsonUtils.Exclude
    public final List<Tuple2<String, byte[]>> byteCode;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ProgramSubmissionEvent() { this(null, null, null); }
    public ProgramSubmissionEvent(final MachineDescriptor clientMachine,
                                  final UUID programID,
                                  final List<Tuple2<String, byte[]>> byteCode) {

        super(PSERVER_JOB_SUBMISSION_EVENT);

        this.clientMachine      = clientMachine;
        this.programID          = programID;
        this.byteCode           = byteCode; //Collections.unmodifiableList(byteCode);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public String toString() { return "\nProgramSubmissionEvent " + gson.toJson(this); }
}
