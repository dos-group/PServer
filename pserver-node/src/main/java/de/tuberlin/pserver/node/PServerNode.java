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

public final class PServerNode extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final MachineDescriptor machine;

    private final InfrastructureManager infraManager;

    private final NetManager netManager;

    private final UserCodeManager userCodeManager;

    private final DataManager dataManager;

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
        this.dataManager        = factory.dataManager;
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

            final ProgramContext programContext = new ProgramContext(
                    runtimeContext,
                    programSubmission.clientMachine,
                    programSubmission.programID,
                    clazz.getName(),
                    clazz.getSimpleName(),
                    infraManager.getMachines().size()
            );

            dataManager.registerProgram(programContext);

            new Thread(() -> {

                final long systemThreadID = Thread.currentThread().getId();

                try {

                    final Program programInvokeable = programClass.newInstance();

                    final SlotContext slotContext = new SlotContext(
                            runtimeContext,
                            programContext,
                            0,
                            programInvokeable
                    );

                    programContext.addSlotContext(systemThreadID, slotContext);

                    programInvokeable.injectContext(slotContext);

                    programInvokeable.run();

                    final List<Serializable> results = dataManager.getResults(slotContext.programContext.programID);

                    final ProgramResultEvent jre = new ProgramResultEvent(
                            slotContext.runtimeContext.machine,
                            slotContext.runtimeContext.nodeID,
                            slotContext.programContext.programID,
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

                    dataManager.clearContext();

                    programContext.removeSlotContext(systemThreadID);

                    dataManager.unregisterProgram(programContext);
                }
            }).start();
        }
    }
}
