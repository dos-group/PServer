package de.tuberlin.pserver.node;

import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.runtime.*;
import de.tuberlin.pserver.runtime.dht.DHTManager;
import de.tuberlin.pserver.runtime.events.ProgramFailureEvent;
import de.tuberlin.pserver.runtime.events.ProgramResultEvent;
import de.tuberlin.pserver.runtime.events.ProgramSubmissionEvent;
import de.tuberlin.pserver.runtime.usercode.UserCodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class PServerNode extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(PServerNode.class);

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
        this.executor           = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        this.runtimeContext = new RuntimeContext(
                machine,
                infraManager.getMachines().size(),
                executionManager.getNumOfSlots(),
                infraManager.getNodeID(),
                netManager,
                DHTManager.getInstance(),
                dataManager,
                executionManager
        );

        netManager.addEventListener(ProgramSubmissionEvent.PSERVER_JOB_SUBMISSION_EVENT, new PServerJobHandler());
    }

    // ---------------------------------------------------
    // Event Handler.
    // ---------------------------------------------------

    private final class PServerJobHandler implements IEventHandler {

        @Override
        public void handleEvent(final Event e) {
            final ProgramSubmissionEvent programSubmission = (ProgramSubmissionEvent)e;
            LOG.info("Received job on instance " + "[" + infraManager.getNodeID() + "]" + programSubmission.toString());

            final Class<?> clazz = userCodeManager.implantClass(programSubmission.byteCode);

            if (!MLProgram.class.isAssignableFrom(clazz))
                throw new IllegalStateException();

            if (programSubmission.perNodeDOP > executionManager.getNumOfSlots())
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

            LOG.info(programContext.toString());

            executionManager.registerJob(programContext.programID, programContext);

            for (int i = 0; i < programContext.perNodeDOP; ++i) {
                final int threadID = i;
                executor.execute(() -> {
                    try {

                        final MLProgram programInvokeable = programClass.newInstance();

                        final SlotContext slotContext = new SlotContext(
                                programContext,
                                threadID,
                                programInvokeable
                        );

                        executionManager.registerSlotContext(slotContext);
                        programContext.addSlot(slotContext);

                        programInvokeable.injectContext(slotContext);

                        programInvokeable.run();

                        if (slotContext.slotID == 0) {

                            final List<Serializable> results = dataManager.getResults(slotContext.programContext.programID);

                            final ProgramResultEvent jre = new ProgramResultEvent(
                                    slotContext.programContext.runtimeContext.machine,
                                    slotContext.programContext.runtimeContext.nodeID,
                                    slotContext.programContext.programID,
                                    results
                            );

                            slotContext.programContext.runtimeContext.netManager.sendEvent(slotContext.programContext.clientMachine, jre);

                            executionManager.unregisterJob(slotContext.programContext.programID);
                        }

                        programContext.removeSlot(slotContext);
                        executionManager.unregisterSlotContext();

                    } catch (Exception ex) {
                        final ProgramFailureEvent jfe = new ProgramFailureEvent(
                                machine,
                                programSubmission.programID,
                                infraManager.getNodeID(),
                                threadID, clazz.getSimpleName(),
                                ex.getCause()
                        );
                        netManager.sendEvent(programSubmission.clientMachine, jfe);
                    }
                });
            }
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void deactivate() {
        netManager.deactivate();
        infraManager.deactivate();
        super.deactivate();
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /*private void executeLifecycle(final MLProgram program) {
        try {

            if (program.slotContext.slotID == 0) {

                LOG.info("Enter " + program.slotContext.programContext.simpleClassName + " prologue phase.");

                final long start = System.currentTimeMillis();

                program.prologue();

                dataManager.loadInputData(program.slotContext);

                final long end = System.currentTimeMillis();

                LOG.info("Leave " + program.slotContext.programContext.simpleClassName
                        + " prologue phase [duration: " + (end - start) + " ms].");

                Thread.sleep(5000); // TODO: Not very elegant...

                jobStartBarrier.countDown();
            }

            jobStartBarrier.await();

            {
                LOG.info("Enter " + program.slotContext.programContext.simpleClassName + " computation phase.");

                final long start = System.currentTimeMillis();

                program.compute();

                final long end = System.currentTimeMillis();

                LOG.info("Leave " + program.slotContext.programContext.simpleClassName +
                        " computation phase [duration: " + (end - start) + " ms].");
            }

            if (program.slotContext.slotID == 0) {

                LOG.info("Enter " + program.slotContext.programContext.simpleClassName + " epilogue phase.");

                final long start = System.currentTimeMillis();

                program.epilogue();

                final long end = System.currentTimeMillis();

                LOG.info("Leave " + program.slotContext.programContext.simpleClassName
                        + " epilogue phase [duration: " + (end - start) + " ms].");
            }

            jobEndBarrier.countDown();

        } catch (Throwable t) {
            throw new IllegalStateException(t);
        }
    }*/
}
