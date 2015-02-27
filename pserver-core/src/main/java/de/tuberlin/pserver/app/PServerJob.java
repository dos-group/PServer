package de.tuberlin.pserver.app;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public final class PServerJob implements Serializable {

    public static final String PSERVER_SUBMIT_JOB_EVENT  = "PSJE";

    public static final String PSERVER_FINISH_JOB_EVENT  = "PFJE";

    private static final long serialVersionUID = -1L;

    public final String className;

    public final String simpleClassName;

    public final List<String> classDependencies;

    public final byte[] classByteCode;

    public PServerJob(final String className, final String simpleClassName, final List<String> classDependencies, final byte[] byteCode) {
        this.className          = Preconditions.checkNotNull(className);
        this.simpleClassName    = Preconditions.checkNotNull(simpleClassName);
        this.classDependencies  = Collections.unmodifiableList(classDependencies);
        this.classByteCode      = Preconditions.checkNotNull(byteCode);
    }
}
