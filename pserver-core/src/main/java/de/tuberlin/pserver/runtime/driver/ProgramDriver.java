package de.tuberlin.pserver.runtime.driver;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.compiler.Compiler;
import de.tuberlin.pserver.compiler.*;
import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.math.matrix.MatrixBase;
import de.tuberlin.pserver.runtime.RuntimeContext;
import de.tuberlin.pserver.runtime.core.common.Deactivatable;
import de.tuberlin.pserver.runtime.core.infra.InfrastructureManager;
import de.tuberlin.pserver.runtime.core.usercode.UserCodeManager;
import de.tuberlin.pserver.runtime.events.ProgramSubmissionEvent;
import org.apache.commons.lang3.ArrayUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

public class ProgramDriver implements Deactivatable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final InfrastructureManager infraManager;

    private final UserCodeManager userCodeManager;

    private final RuntimeContext runtimeContext;

    private final StateAllocator stateAllocator;

    // ---------------------------------------------------

    private ProgramContext programContext;

    private ProgramTable programTable;

    private Program instance;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public ProgramDriver(final InfrastructureManager infraManager,
                         final UserCodeManager userCodeManager,
                         final RuntimeContext runtimeContext) {

        this.infraManager = Preconditions.checkNotNull(infraManager);

        this.userCodeManager = Preconditions.checkNotNull(userCodeManager);

        this.runtimeContext = Preconditions.checkNotNull(runtimeContext);

        this.stateAllocator = new StateAllocator(runtimeContext.netManager, runtimeContext.fileManager);
    }

    public void deactivate() {

        stateAllocator.clearContext();

        programContext.deactivate();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    public Program install(final ProgramSubmissionEvent submissionEvent) throws Exception {
        Preconditions.checkNotNull(submissionEvent);

        final int nodeDOP = infraManager.getMachines().size();
        final Class<? extends Program> programClass = (Class<? extends Program>) userCodeManager.implantClass(submissionEvent.byteCode);
        this.instance = programClass.newInstance();
        this.programTable = new Compiler(runtimeContext, programClass).compile(instance, nodeDOP);

        this.programContext = new ProgramContext(
                runtimeContext,
                submissionEvent.clientMachine,
                submissionEvent.programID,
                programClass.getName(),
                programClass.getSimpleName(),
                programTable,
                nodeDOP
        );

        instance.injectContext(programContext);

        return instance;
    }

    public void run() throws Exception {

        allocateState();

        bindState(instance);

        bindTransactions();

        defineProgram(instance);

        instance.run();
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void allocateState() throws Exception {
        for (final StateDescriptor state : programContext.programTable.getState()) {
            if (MatrixBase.class.isAssignableFrom(state.stateType)) {
                final MatrixBase m = stateAllocator.alloc(programContext, state);
                if (m != null)
                    runtimeContext.runtimeManager.putDHT(state.stateName, m);
            } else
                throw new IllegalStateException();
        }

        try {
            Thread.sleep(2000); // TODO: REMOVE !!!
        } catch (InterruptedException ex) {
            //throw new IllegalStateException(ex);
        }

        stateAllocator.loadData(programContext);
    }

    private void bindState(final Program instance) throws Exception {
        for (final StateDescriptor state : programTable.getState()) {
            final Field field = programTable.getProgramClass().getDeclaredField(state.stateName);
            final Object stateObj = runtimeContext.runtimeManager.getDHT(state.stateName);
            Preconditions.checkState(stateObj != null, "State object '" + state.stateName + "' not found.");
            field.set(instance, stateObj);
        }
    }

    private void bindTransactions() throws Exception {
        for (final TransactionController transactionController : programTable.getTransactionControllers()) {
            transactionController.bindTransaction();
        }
    }

    private void defineProgram(final Program instance) {
        for (final UnitDescriptor unit : programTable.getUnits()) {
            if (ArrayUtils.contains(unit.atNodes, runtimeContext.nodeID)) {
                try {
                    unit.method.invoke(instance, instance.getLifecycle());
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }
}
