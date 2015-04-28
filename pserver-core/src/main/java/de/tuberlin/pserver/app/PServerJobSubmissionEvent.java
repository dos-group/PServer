package de.tuberlin.pserver.app;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.utils.GsonUtils;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public final class PServerJobSubmissionEvent extends NetEvents.NetEvent {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String PSERVER_JOB_SUBMISSION_EVENT = "PSERVER_JOB_SUBMISSION_EVENT";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private transient static Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

    private static final long serialVersionUID = -1L;

    public final MachineDescriptor clientMachine;

    public final UUID jobUID;

    public final String className;

    public final String simpleClassName;

    public final int perNodeParallelism;

    @GsonUtils.Exclude
    public final List<String> classDependencies;

    @GsonUtils.Exclude
    public final byte[] classByteCode;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PServerJobSubmissionEvent(final MachineDescriptor clientMachine,
                                     final UUID jobUID,
                                     final String className,
                                     final String simpleClassName,
                                     final int perNodeParallelism,
                                     final List<String> classDependencies,
                                     final byte[] byteCode) {

        super(PSERVER_JOB_SUBMISSION_EVENT);

        this.clientMachine      = Preconditions.checkNotNull(clientMachine);
        this.jobUID             = Preconditions.checkNotNull(jobUID);
        this.className          = Preconditions.checkNotNull(className);
        this.simpleClassName    = Preconditions.checkNotNull(simpleClassName);
        this.classDependencies  = Collections.unmodifiableList(Preconditions.checkNotNull(classDependencies));
        this.classByteCode      = Preconditions.checkNotNull(byteCode);
        this.perNodeParallelism = perNodeParallelism;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public String toString() { return "\nPServerJobSubmissionEvent " + gson.toJson(this); }
}
