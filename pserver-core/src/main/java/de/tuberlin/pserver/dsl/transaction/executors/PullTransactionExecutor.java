package de.tuberlin.pserver.dsl.transaction.executors;

import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.events.TransactionRequestEvent;
import de.tuberlin.pserver.dsl.transaction.events.TransactionResponseEvent;
import de.tuberlin.pserver.dsl.transaction.phases.Prepare;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.runtime.RuntimeContext;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;


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

    private final int numStateObjects;

    private CountDownLatch responseLatch;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PullTransactionExecutor(final RuntimeContext runtimeContext,
                                   final TransactionController controller) {

        super(runtimeContext, controller);

        this.transactionDefinition = controller.getTransactionDescriptor().definition;

        this.numStateObjects = controller.getTransactionDescriptor().stateObjectNames.size();

        this.remoteStateObjects = new SharedObject[numStateObjects];

        this.localStateObjects = new SharedObject[numStateObjects];

        this.responseLatch = new CountDownLatch(controller.getTransactionDescriptor().latchCount);

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

            int index = 0;
            for (final String stateObjectName : controller.getTransactionDescriptor().stateObjectNames) {

                final String trEventName = transactionName + "_" + stateObjectName;

                final boolean cacheRequest = controller.getTransactionDescriptor().cacheRequestObject;

                final TransactionRequestEvent request = new TransactionRequestEvent(
                        trEventName,
                        requestObject,
                        cacheRequest
                );

                runtimeContext.netManager.sendEvent(controller.getTransactionDescriptor().stateObjectNodes.get(index), request);

                ++index;
            }
        }

        { // Await Response...

            responseLatch.await();

            synchronized (monitor) {
                responseLatch = new CountDownLatch(controller.getTransactionDescriptor().latchCount);
            }
        }

        { // Response Handling...

            int index = 0;
            for (final String stateObjectName : controller.getTransactionDescriptor().stateObjectNames) {
                resultObjects.add(
                        transactionDefinition.applyPhase.apply(
                                responseObjects.get(stateObjectName),
                                localStateObjects[index]
                        )
                );
                ++index;
            }
        }

        return resultObjects;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void registerTransactionHandlers() {

        int i = 0;
        for (final String stateObjectName : controller.getTransactionDescriptor().stateObjectNames) {

            final String trEventName = transactionName + "_" + stateObjectName;

            // ---------------------------------------------------

            final int index = i;

            runtimeContext.netManager.addEventListener(TransactionRequestEvent.TRANSACTION_REQUEST + trEventName, event -> {
                final TransactionRequestEvent request = (TransactionRequestEvent) event;

                try {

                    if (remoteStateObjects[index] == null) {
                        remoteStateObjects[index] = runtimeContext.runtimeManager.getDHT(stateObjectName);
                    }

                    remoteStateObjects[index].lock();
                    final Prepare preparePhase = transactionDefinition.preparePhase;
                    final Object prepareInput = request.getPayload() == null ? remoteStateObjects[index] : request.requestObject;
                    final Object prepareOutput = (preparePhase != null) ? preparePhase.prepare(prepareInput) : prepareInput;

                    runtimeContext.netManager.sendEvent(
                            request.srcMachineID,
                            new TransactionResponseEvent(trEventName, prepareOutput)
                    );

                    remoteStateObjects[index].unlock();

                } catch (Exception ex) {
                    throw new IllegalStateException(ex);
                }
            });

            // ---------------------------------------------------

            runtimeContext.netManager.addEventListener(TransactionResponseEvent.TRANSACTION_RESPONSE + trEventName, event -> {

                final TransactionResponseEvent response = (TransactionResponseEvent) event;
                List<Object> responseList = responseObjects.get(stateObjectName);

                if (responseList == null) {
                    responseList = new ArrayList<>();
                    responseObjects.put(stateObjectName, responseList);
                }

                responseList.add(response.responseObject);
                synchronized (monitor) {
                    responseLatch.countDown();
                }
            });

            // ---------------------------------------------------

            ++i;
        }
    }
}
