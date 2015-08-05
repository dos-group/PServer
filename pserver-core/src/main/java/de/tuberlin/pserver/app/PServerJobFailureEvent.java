package de.tuberlin.pserver.app;


import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.utils.GsonUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.util.UUID;

public final class PServerJobFailureEvent extends NetEvents.NetEvent {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String PSERVER_FAILURE_JOB_EVENT = "PSERVER_FAILURE_JOB_EVENT";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private transient static Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

    private static final long serialVersionUID = -1L;

    public final MachineDescriptor machine;

    public final UUID jobUID;

    public final int nodeID;

    public final int instanceID;

    public final String jobName;

    @GsonUtils.Exclude
    public final String stackTrace;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PServerJobFailureEvent(final MachineDescriptor machine,
                                  final UUID jobUID,
                                  final int nodeID,
                                  final int instanceID,
                                  final String jobName,
                                  final Throwable exception) {

        super(PSERVER_FAILURE_JOB_EVENT);

        this.machine    = Preconditions.checkNotNull(machine);
        this.jobUID     = Preconditions.checkNotNull(jobUID);
        this.nodeID     = nodeID;
        this.instanceID = instanceID;
        this.jobName    = Preconditions.checkNotNull(jobName);
        this.stackTrace = ExceptionUtils.getStackTrace(Preconditions.checkNotNull(exception));
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public String toString() {
        return "\nPServerJobFailureEvent " + gson.toJson(this) + "\n ==> JAVA STACK TRACE [\n" + stackTrace + "]";
    }
}
