package de.tuberlin.pserver.node;

import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.runtime.*;
import de.tuberlin.pserver.runtime.events.ProgramFailureEvent;
import de.tuberlin.pserver.runtime.events.ProgramResultEvent;
import de.tuberlin.pserver.runtime.events.ProgramSubmissionEvent;
import de.tuberlin.pserver.runtime.usercode.UserCodeManager;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class PServerNode extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    //private static final Logger LOG = LoggerFactory.getLogger(PServerNode.class);

    private final MachineDescriptor machine;

    private final InfrastructureManager infraManager;

    private final NetManager netManager;

    private final UserCodeManager userCodeManager;

    private final DataManager dataManager;

    private final ExecutionManager executionManager;

    private final ExecutorService executor;

    private final RuntimeContext runtimeContext;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PServerNode(final PServerNodeFactory factory) {
        super(true, "PSERVER-NODE-THREAD");

        this.machine            = factory.machine;
        this.infraManager       = factory.infraManager;
        this.netManager         = factory.netManager;
        this.userCodeManager    = factory.userCodeManager;
        this.dataManager        = factory.dataManager;
        this.executionManager   = factory.executionManager;
        this.runtimeContext     = factory.runtimeContext;
        this.executor           = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        netManager.addEventListener(ProgramSubmissionEvent.PSERVER_JOB_SUBMISSION_EVENT, new PServerJobHandler());
    }

    @Override
    public void deactivate() {
        executor.shutdownNow();
        dataManager.deactivate();
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
            if (!MLProgram.class.isAssignableFrom(clazz))
                throw new IllegalStateException();
            if (programSubmission.perNodeDOP > runtimeContext.numOfCores)
                throw new IllegalStateException();

            @SuppressWarnings("unchecked")
            final Class<? extends MLProgram> programClass = (Class<? extends MLProgram>) clazz;

            final MLProgramContext programContext = new MLProgramContext(
                    runtimeContext,
                    programSubmission.clientMachine,
                    programSubmission.programID,
                    clazz.getName(),
                    clazz.getSimpleName(),
                    infraManager.getMachines().size(),
                    programSubmission.perNodeDOP
            );

            executionManager.registerProgram(programContext);

            for (int i = 0; i < programContext.perNodeDOP; ++i) {
                final int logicThreadID = i;

                executor.execute(() -> {

                    final long systemThreadID = Thread.currentThread().getId();

                    MLProgram programInvokeable = null;

                    try {

                        programInvokeable = programClass.newInstance();

                        final SlotContext slotContext = new SlotContext(
                                runtimeContext,
                                programContext,
                                logicThreadID,
                                programInvokeable
                        );

                        programContext.addSlotContext(systemThreadID, slotContext);

                        programInvokeable.injectContext(slotContext);

                        programInvokeable.run();

                        if (logicThreadID == 0) {

                            final List<Serializable> results = dataManager.getResults(slotContext.programContext.programID);

                            final ProgramResultEvent jre = new ProgramResultEvent(
                                    slotContext.runtimeContext.machine,
                                    slotContext.runtimeContext.nodeID,
                                    slotContext.programContext.programID,
                                    results
                            );

                            netManager.sendEvent(programSubmission.clientMachine, jre);
                        }

                    } catch (Throwable ex) {

                        final ProgramFailureEvent jfe = new ProgramFailureEvent(
                                machine,
                                programSubmission.programID,
                                infraManager.getNodeID(),
                                logicThreadID,
                                clazz.getSimpleName(),
                                ExceptionUtils.getStackTrace(ex)
                        );

                        netManager.sendEvent(programSubmission.clientMachine, jfe);

                    } finally {

                        if (logicThreadID == 0) {

                            dataManager.clearContext();

                            programContext.removeSlotContext(systemThreadID);

                            executionManager.unregisterProgram(programContext);
                        }

                        if (programInvokeable != null)
                            programInvokeable.deactivate();
                    }
                });
            }
        }
    }
}
