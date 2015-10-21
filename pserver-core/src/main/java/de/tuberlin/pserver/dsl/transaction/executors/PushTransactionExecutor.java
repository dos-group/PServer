package de.tuberlin.pserver.dsl.transaction.executors;

import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.events.TransactionRequestEvent;
import de.tuberlin.pserver.dsl.transaction.phases.Prepare;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.runtime.RuntimeContext;

import java.util.ArrayList;
import java.util.List;

public class PushTransactionExecutor extends TransactionExecutor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final TransactionDefinition transactionDefinition;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PushTransactionExecutor(final RuntimeContext runtimeContext,
                                   final TransactionController controller) {

        super(runtimeContext, controller);

        this.transactionDefinition = controller.getTransactionDescriptor().definition;

        register();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void bind() throws Exception {

    }

    @Override
    public synchronized List<Object> execute(final Object requestObject) throws Exception {
        /*final SharedObject stateObject = runtimeContext.runtimeManager.getDHT(controller.getTransactionDescriptor().stateObjectNames.get(0));
        stateObject.lock();
        final boolean cacheRequest = controller.getTransactionDescriptor().cacheRequestObject;
        final Prepare preparePhase = transactionDefinition.preparePhase;
        final Object preparedStateObject = (preparePhase != null) ? preparePhase.prepare(stateObject) : stateObject;
        final TransactionRequestEvent request = new TransactionRequestEvent(transactionName, preparedStateObject, cacheRequest);
        runtimeContext.netManager.sendEvent(controller.getTransactionDescriptor().stateObjectNodes, request);
        stateObject.unlock();*/
        return null;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void register() {
        runtimeContext.netManager.addEventListener(TransactionRequestEvent.TRANSACTION_REQUEST + transactionName, event -> {
            try {
                final TransactionRequestEvent request = (TransactionRequestEvent) event;
                final SharedObject object = (SharedObject) request.getPayload();
                final SharedObject stateObject = runtimeContext.runtimeManager.getDHT(controller.getTransactionDescriptor().stateObjectNames.get(0));
                final List<SharedObject> remoteObjects = new ArrayList<>();
                remoteObjects.add(object);
                stateObject.lock();
                transactionDefinition.applyPhase.apply(remoteObjects, null);
                stateObject.unlock();
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        });
    }
}
