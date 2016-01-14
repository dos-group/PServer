package de.tuberlin.pserver.dsl.transaction.executors;

import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.events.TransactionPullRequestEvent;
import de.tuberlin.pserver.dsl.transaction.events.TransactionPullResponseEvent;
import de.tuberlin.pserver.dsl.transaction.phases.Prepare;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.runtime.RuntimeContext;

import java.util.*;
import java.util.concurrent.CountDownLatch;

// EXECUTOR ASSUMES NO CONCURRENT TRANSACTIONS OF SAME TYPE.
// MUST BE ENFORCED BY TRANSACTION MANAGER!

public class PullTransactionExecutor extends TransactionExecutor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Object monitor = new Object();

    // ---------------------------------------------------

    private final TransactionDefinition transactionDefinition;

    private final Map<String, List<Object>> responseObjects = new HashMap<>();

    private final SharedObject[] remoteStateObjects;

    private final SharedObject[] localStateObjects;

    private CountDownLatch responseLatch;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PullTransactionExecutor(final RuntimeContext runtimeContext,
                                   final TransactionController controller) {

        super(runtimeContext, controller);

        this.transactionDefinition = controller.getTransactionDescriptor().definition;

        final int numStateObjects = controller.getTransactionDescriptor().stateObjectNames.size();

        this.remoteStateObjects = new SharedObject[numStateObjects];

        this.localStateObjects = new SharedObject[numStateObjects];

        this.responseLatch = new CountDownLatch(controller.getTransactionDescriptor().stateObjectNodes.length);

        registerTransactionHandlers();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void bind() throws Exception {

        int index = 0;

        for (final String stateObjectName : controller.getTransactionDescriptor().stateObjectNames) {

            localStateObjects[index++] = runtimeContext.runtimeManager.getDHT(stateObjectName);
        }
    }

    @Override
    public synchronized List<Object> execute(final Object requestObject) throws Exception {
        responseObjects.clear();

        final List<Object> resultObjects = new ArrayList<>();

        { // Request...

            final boolean cacheRequest = controller.getTransactionDescriptor().cacheRequestObject;

            final TransactionPullRequestEvent request = new TransactionPullRequestEvent(
                    transactionName,
                    controller.getTransactionDescriptor().stateObjectNames,
                    requestObject,
                    cacheRequest
            );

            runtimeContext.netManager.dispatchEventAt(controller.getTransactionDescriptor().stateObjectNodes, request);
        }

        { // Await Response...

            responseLatch.await();

            synchronized (monitor) {
                responseLatch = new CountDownLatch(controller.getTransactionDescriptor().stateObjectNodes.length);
            }
        }

        { // Response Handling...

            for (int i = 0; i < controller.getTransactionDescriptor().stateObjectNames.size(); ++i) {
                resultObjects.add(
                        transactionDefinition.applyPhase.apply(
                                responseObjects.get(controller.getTransactionDescriptor().stateObjectNames.get(i)),
                                localStateObjects[i]
                        )
                );
            }
        }

        return resultObjects;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void registerTransactionHandlers() {

        runtimeContext.netManager.addEventListener(TransactionPullRequestEvent.TRANSACTION_REQUEST + transactionName, event -> {
            final TransactionPullRequestEvent request = (TransactionPullRequestEvent) event;

            try {

                final List<Object> preparedOutputs = new ArrayList<>();
                for (int i = 0; i < request.stateObjectNames.size(); ++i) {

                    if (remoteStateObjects[i] == null) {
                        remoteStateObjects[i] = runtimeContext.runtimeManager.getDHT(request.stateObjectNames.get(i));
                    }

                    remoteStateObjects[i].lock();
                    final Prepare preparePhase = transactionDefinition.preparePhase;
                    final Object prepareInput = request.getPayload() == null ? remoteStateObjects[i] : request.requestObject;
                    preparedOutputs.add((preparePhase != null) ? preparePhase.prepare(prepareInput) : prepareInput);
                    remoteStateObjects[i].unlock();
                }

                runtimeContext.netManager.dispatchEventAt(
                        request.srcMachineID,
                        new TransactionPullResponseEvent(transactionName, preparedOutputs)
                );

            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        });

        // ---------------------------------------------------

        // NO GUARANTEE ABOUT RESPONSE ORDER IS EQUAL TO REQUEST ORDER!

        runtimeContext.netManager.addEventListener(TransactionPullResponseEvent.TRANSACTION_RESPONSE + transactionName, event -> {

            final TransactionPullResponseEvent response = (TransactionPullResponseEvent) event;
            for (int i = 0; i < controller.getTransactionDescriptor().stateObjectNames.size(); ++i) {

                final String stateObjectName = controller.getTransactionDescriptor().stateObjectNames.get(i);

                List<Object> responseList = responseObjects.get(stateObjectName);

                if (responseList == null) {
                    responseList = new ArrayList<>();
                    responseObjects.put(stateObjectName, responseList);
                }

                responseList.add(response.responseObjects.get(i));
            }

            synchronized (monitor) {
                responseLatch.countDown();
            }
        });

        // ---------------------------------------------------
    }
}
