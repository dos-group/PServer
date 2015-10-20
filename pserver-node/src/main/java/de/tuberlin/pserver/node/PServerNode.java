package de.tuberlin.pserver.node;

import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.runtime.RuntimeContext;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.core.events.Event;
import de.tuberlin.pserver.runtime.core.events.EventDispatcher;
import de.tuberlin.pserver.runtime.core.events.IEventHandler;
import de.tuberlin.pserver.runtime.core.infra.InfrastructureManager;
import de.tuberlin.pserver.runtime.core.infra.MachineDescriptor;
import de.tuberlin.pserver.runtime.core.net.NetManager;
import de.tuberlin.pserver.runtime.driver.ProgramDriver;
import de.tuberlin.pserver.runtime.events.ProgramFailureEvent;
import de.tuberlin.pserver.runtime.events.ProgramResultEvent;
import de.tuberlin.pserver.runtime.events.ProgramSubmissionEvent;
import de.tuberlin.pserver.runtime.parallel.ParallelRuntime;
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

    private final RuntimeManager runtimeManager;

    private final RuntimeContext runtimeContext;

    private final ProgramDriver driver;

    // ---------------------------------------------------
    // Constructors / Deactivator.
    // ---------------------------------------------------

    public PServerNode(final PServerNodeFactory factory) {
        super(true, "PSERVER-NODE-THREAD");

        this.machine            = factory.machine;
        this.infraManager       = factory.infraManager;
        this.netManager         = factory.netManager;
        this.runtimeManager     = factory.runtimeManager;
        this.runtimeContext     = factory.runtimeContext;

        this.driver = new ProgramDriver(infraManager, factory.userCodeManager, runtimeContext);

        netManager.addEventListener(ProgramSubmissionEvent.PSERVER_JOB_SUBMISSION_EVENT, new PServerJobHandler());
    }

    @Override
    public void deactivate() {

        runtimeManager.deactivate();

        netManager.deactivate();

        infraManager.deactivate();

        super.deactivate();
    }

    // ---------------------------------------------------
    // Event Handler.
    // ---------------------------------------------------

    private final class PServerJobHandler implements IEventHandler {

        @Override
        public void handleEvent(final Event event) {

            final ProgramSubmissionEvent programSubmission = (ProgramSubmissionEvent)event;

            try {

                final Program instance = driver.install(programSubmission);

                new Thread(() -> {

                    try {

                        ParallelRuntime.INSTANCE.create(runtimeContext.numOfCores);

                        driver.run();

                        final List<Serializable> results = instance.programContext.getResults();

                        final ProgramResultEvent jre = new ProgramResultEvent(
                                runtimeContext.machine,
                                runtimeContext.nodeID,
                                instance.programContext.programID,
                                results
                        );

                        netManager.sendEvent(programSubmission.clientMachine, jre);

                    } catch (Throwable ex) {

                        final ProgramFailureEvent jfe = new ProgramFailureEvent(
                                machine,
                                programSubmission.programID,
                                infraManager.getNodeID(),
                                0,
                                instance.getClass().getSimpleName(),
                                ExceptionUtils.getStackTrace(ex)
                        );

                        netManager.sendEvent(programSubmission.clientMachine, jfe);

                    } finally {

                        driver.deactivate();

                        runtimeManager.clearContext();

                        ParallelRuntime.INSTANCE.deactivate();
                    }

                }).start();

            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        }
    }
}
