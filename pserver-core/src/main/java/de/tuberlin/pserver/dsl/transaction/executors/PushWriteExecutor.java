package de.tuberlin.pserver.dsl.transaction.executors;


import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.dsl.transaction.phases.Prepare;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.runtime.RuntimeContext;
import de.tuberlin.pserver.runtime.dht.types.EmbeddedDHTObject;

import java.util.ArrayList;
import java.util.List;

public class PushWriteExecutor extends TransactionExecutor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final String requestTransactionID;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PushWriteExecutor(final RuntimeContext runtimeContext,
                             final TransactionController controller) {

        super(runtimeContext, controller);
        final String name = controller.getTransactionDeclaration().transactionName;
        this.requestTransactionID  = PUSH_WRITE_TRANSACTION_REQUEST + "_" + name;
        register();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void execute() throws Exception {
        final SharedObject stateObject = ((EmbeddedDHTObject)runtimeContext.dataManager.getLocal(controller.getTransactionDeclaration().stateName)[0]).object;
        stateObject.lock();
        final Prepare preparePhase = controller.getTransactionDefinition().preparePhase;
        final Object preparedStateObject = (preparePhase != null) ? preparePhase.prepare(stateObject) : stateObject;
        final NetEvents.NetEvent request = new NetEvents.NetEvent(requestTransactionID);
        request.setPayload(preparedStateObject);
        runtimeContext.netManager.sendEvent(controller.getTransactionDeclaration().nodes, request);
        stateObject.unlock();
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void register() {
        runtimeContext.netManager.addEventListener(requestTransactionID, event -> {
            try {
                final NetEvents.NetEvent request = (NetEvents.NetEvent) event;
                final SharedObject object = (SharedObject) request.getPayload();
                final SharedObject stateObject = ((EmbeddedDHTObject)runtimeContext.dataManager.getLocal(controller.getTransactionDeclaration().stateName)[0]).object;
                final List<SharedObject> remoteObjects = new ArrayList<>();
                remoteObjects.add(object);
                stateObject.lock();
                controller.getTransactionDefinition().applyPhase.apply(remoteObjects);
                stateObject.unlock();
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        });
    }
}
