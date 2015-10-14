package de.tuberlin.pserver.node;

import de.tuberlin.pserver.compiler.*;
import de.tuberlin.pserver.compiler.Compiler;
import de.tuberlin.pserver.runtime.ProgramContext;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.runtime.RuntimeContext;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.events.ProgramFailureEvent;
import de.tuberlin.pserver.runtime.events.ProgramResultEvent;
import de.tuberlin.pserver.runtime.events.ProgramSubmissionEvent;
import de.tuberlin.pserver.runtime.mcruntime.MCRuntime;
import de.tuberlin.pserver.runtime.usercode.UserCodeManager;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.Serializable;
import java.util.List;

public final class PServerNode extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final MachineDescriptor machine;

    private final InfrastructureManager infraManager;

    private final NetManager netManager;

    private final UserCodeManager userCodeManager;

    private final RuntimeManager runtimeManager;

    private final RuntimeContext runtimeContext;

    // ---------------------------------------------------
    // Constructors / Deactivator.
    // ---------------------------------------------------

    public PServerNode(final PServerNodeFactory factory) {
        super(true, "PSERVER-NODE-THREAD");

        this.machine            = factory.machine;
        this.infraManager       = factory.infraManager;
        this.netManager         = factory.netManager;
        this.userCodeManager    = factory.userCodeManager;
        this.runtimeManager     = factory.runtimeManager;
        this.runtimeContext     = factory.runtimeContext;

        netManager.addEventListener(ProgramSubmissionEvent.PSERVER_JOB_SUBMISSION_EVENT, new PServerJobHandler());
    }

    @Override
    public void deactivate() {

        netManager.deactivate();

        infraManager.deactivate();

        super.deactivate();
    }

    // ---------------------------------------------------
    // Event Handler.
    // ---------------------------------------------------

    private final class PServerJobHandler implements IEventHandler {

        @Override
        public void handleEvent(final Event e) {

            final ProgramSubmissionEvent programSubmission = (ProgramSubmissionEvent)e;

            final Class<?> clazz = userCodeManager.implantClass(programSubmission.byteCode);

            if (!Program.class.isAssignableFrom(clazz))
                throw new IllegalStateException();

            @SuppressWarnings("unchecked")
            final Class<? extends Program> programClass = (Class<? extends Program>) clazz;

            final Program instance;

            try {
                instance = programClass.newInstance();

                final int nodeDOP = infraManager.getMachines().size();

                final ProgramTable programTable = new Compiler(runtimeContext, programClass).compile(instance, nodeDOP);

                final ProgramContext programContext = new ProgramContext(
                        runtimeContext,
                        programSubmission.clientMachine,
                        programSubmission.programID,
                        clazz.getName(),
                        clazz.getSimpleName(),
                        programTable,
                        nodeDOP
                );

                new Thread(() -> {

                    try {

                        MCRuntime.INSTANCE.create(runtimeContext.numOfCores);

                        instance.injectContext(programContext);

                        runtimeManager.bind(instance);

                        instance.run();

                        final List<Serializable> results = programContext.getResults();

                        final ProgramResultEvent jre = new ProgramResultEvent(
                                runtimeContext.machine,
                                runtimeContext.nodeID,
                                programContext.programID,
                                results
                        );

                        netManager.sendEvent(programSubmission.clientMachine, jre);

                    } catch (Throwable ex) {

                        final ProgramFailureEvent jfe = new ProgramFailureEvent(
                                machine,
                                programSubmission.programID,
                                infraManager.getNodeID(),
                                0,
                                clazz.getSimpleName(),
                                ExceptionUtils.getStackTrace(ex)
                        );

                        netManager.sendEvent(programSubmission.clientMachine, jfe);

                    } finally {

                        runtimeManager.clearContext();

                        programContext.deactivate();

                        MCRuntime.INSTANCE.deactivate();
                    }

                }).start();

            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        }
    }
}
