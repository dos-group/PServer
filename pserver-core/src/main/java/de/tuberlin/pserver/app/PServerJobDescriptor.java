package de.tuberlin.pserver.app;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.infra.MachineDescriptor;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public final class PServerJobDescriptor implements Serializable {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String PSERVER_SUBMIT_JOB_EVENT  = "PSJE";

    public static final String PSERVER_FINISH_JOB_EVENT  = "PFJE";

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    private static final long serialVersionUID = -1L;

    public final MachineDescriptor clientMachine;

    public final UUID jobUID;

    public final String className;

    public final String simpleClassName;

    public final List<String> classDependencies;

    public final byte[] classByteCode;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PServerJobDescriptor(final MachineDescriptor clientMachine,
                                final UUID jobUID,
                                final String className,
                                final String simpleClassName,
                                final List<String> classDependencies,
                                final byte[] byteCode) {

        this.clientMachine      = Preconditions.checkNotNull(clientMachine);
        this.jobUID             = Preconditions.checkNotNull(jobUID);
        this.className          = Preconditions.checkNotNull(className);
        this.simpleClassName    = Preconditions.checkNotNull(simpleClassName);
        this.classDependencies  = Collections.unmodifiableList(Preconditions.checkNotNull(classDependencies));
        this.classByteCode      = Preconditions.checkNotNull(byteCode);
    }
}
