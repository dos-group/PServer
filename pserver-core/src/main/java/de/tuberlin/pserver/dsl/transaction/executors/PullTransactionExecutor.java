package de.tuberlin.pserver.dsl.transaction.executors;

import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.events.TransactionRequestEvent;
import de.tuberlin.pserver.dsl.transaction.events.TransactionResponseEvent;
import de.tuberlin.pserver.dsl.transaction.phases.Prepare;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.runtime.RuntimeContext;
import de.tuberlin.pserver.runtime.dht.types.EmbeddedDHTObject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class PullTransactionExecutor extends TransactionExecutor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final TransactionDefinition transactionDefinition;

    private final List<Object> resultObjects = new ArrayList<>();

    //private Object cachedRequestObject;

    private CountDownLatch responseLatch;

    // ---------------------------------------------------

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PullTransactionExecutor(final RuntimeContext runtimeContext,
                                   final TransactionController controller) {

        super(runtimeContext, controller);

        this.transactionDefinition = controller.getTransactionDescriptor().definition;

        register();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public synchronized Object execute(final Object requestObject) throws Exception {
        resultObjects.clear();
        final boolean cacheRequest = controller.getTransactionDescriptor().cacheRequestObject;
        final TransactionRequestEvent request = new TransactionRequestEvent(
                transactionName,
                requestObject,
                cacheRequest
        );
        runtimeContext.netManager.sendEvent(controller.getTransactionDescriptor().nodes, request);
        responseLatch = new CountDownLatch(controller.getTransactionDescriptor().nodes.length);
        responseLatch.await();
        return transactionDefinition.applyPhase.apply(resultObjects);
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void register() {

        runtimeContext.netManager.addEventListener(TransactionRequestEvent.TRANSACTION_REQUEST + transactionName, event -> {
            final TransactionRequestEvent request = (TransactionRequestEvent)event;

            try {
                final SharedObject stateObject = ((EmbeddedDHTObject) runtimeContext.runtimeManager.getDHT(controller.getTransactionDescriptor().stateName)).object;
                stateObject.lock();
                final Prepare preparePhase = transactionDefinition.preparePhase;
                final Object prepareInput = request.getPayload() == null ? stateObject : request.requestObject;
                final Object prepareOutput = (preparePhase != null) ? preparePhase.prepare(prepareInput) : prepareInput;
                runtimeContext.netManager.sendEvent(
                        request.srcMachineID,
                        new TransactionResponseEvent(transactionName, prepareOutput)
                );
                stateObject.unlock();
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        });

        runtimeContext.netManager.addEventListener(TransactionResponseEvent.TRANSACTION_RESPONSE + transactionName, event -> {
            final TransactionResponseEvent response = (TransactionResponseEvent)event;
            resultObjects.add(response.responseObject);
            responseLatch.countDown();
        });
    }
}
