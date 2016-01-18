package de.tuberlin.pserver.dsl.transaction.executors;

import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.events.TransactionPushRequestEvent;
import de.tuberlin.pserver.dsl.transaction.phases.Prepare;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.runtime.RuntimeContext;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class PushTransactionExecutor extends TransactionExecutor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final TransactionDefinition transactionDefinition;

    private final SharedObject[] localSrcStateObjects;

    private final SharedObject[] localDstStateObjects;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PushTransactionExecutor(final RuntimeContext runtimeContext,
                                   final TransactionController controller) {

        super(runtimeContext, controller);
        this.transactionDefinition = controller.getTransactionDescriptor().definition;

        final int numSrcStateObjects = controller.getTransactionDescriptor().stateSrcObjectNames.size();
        this.localSrcStateObjects = new SharedObject[numSrcStateObjects];

        final int numDstStateObjects = controller.getTransactionDescriptor().stateDstObjectNames.size();
        this.localDstStateObjects = new SharedObject[numDstStateObjects];

        registerPushTransactionRequest();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void bind() throws Exception {
        int i = 0;
        for (final String srcStateObjectName : controller.getTransactionDescriptor().stateSrcObjectNames) {
            if (ArrayUtils.contains(controller.getTransactionDescriptor().srcStateObjectNodes, runtimeContext.nodeID)) {
                localSrcStateObjects[i++] = runtimeContext.runtimeManager.getDHT(srcStateObjectName);
            }
        }
        i = 0;
        for (final String dstStateObjectName : controller.getTransactionDescriptor().stateDstObjectNames) {
            if (ArrayUtils.contains(controller.getTransactionDescriptor().dstStateObjectNodes, runtimeContext.nodeID)) {
                localDstStateObjects[i++] = runtimeContext.runtimeManager.getDHT(dstStateObjectName);
            }
        }
    }

    @Override
    public synchronized List<Object> execute(final Object requestObject) throws Exception {

        int[] txnDstNodes = ArrayUtils.removeElements(
                controller.getTransactionDescriptor().dstStateObjectNodes,
                runtimeContext.nodeID
        );

        if (txnDstNodes.length == 0)
            return null;

        //
        // Apply prepare phase on src-state object.
        //

        final List<Object> preparedSrcStateObjects = new ArrayList<>();
        for (int i = 0; i < controller.getTransactionDescriptor().stateSrcObjectNames.size(); ++i) {
            localSrcStateObjects[i].lock();
            final Prepare preparePhase = transactionDefinition.preparePhase;
            preparedSrcStateObjects.add((preparePhase != null) ? preparePhase.prepare(localSrcStateObjects[i]) : localSrcStateObjects[i]);
            localSrcStateObjects[i].unlock();
        }

        final TransactionPushRequestEvent request = new TransactionPushRequestEvent(
                transactionName,
                controller.getTransactionDescriptor().stateDstObjectNames,
                preparedSrcStateObjects,
                requestObject,
                controller.getTransactionDescriptor().cacheRequestObject
        );

        runtimeContext.netManager.dispatchEventAt(txnDstNodes, request);

        return null;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void registerPushTransactionRequest() {
        // Register push request listener only at the associated destination nodes.
        if (ArrayUtils.contains(controller.getTransactionDescriptor().dstStateObjectNodes, runtimeContext.nodeID)) {

            if (transactionDefinition.combinePhase != null) { // USE COMBINER

                AtomicInteger requestIndex = new AtomicInteger(0);
                int numRequests = controller.getTransactionDescriptor().srcStateObjectNodes.length;
                List<Object> srcStateObjects = new ArrayList<>(numRequests);

                runtimeContext.netManager.addEventListener(TransactionPushRequestEvent.TRANSACTION_REQUEST + transactionName, event -> {
                    final TransactionPushRequestEvent request = (TransactionPushRequestEvent) event;
                    srcStateObjects.addAll(request.srcStateObjectsValues);
                    if (requestIndex.incrementAndGet() == numRequests) {
                        new Thread(() -> {
                            try {
                                Object combinedSrcStateObject = transactionDefinition.combinePhase.combine(srcStateObjects);
                                for (int i = 0; i < controller.getTransactionDescriptor().stateDstObjectNames.size(); ++i) {
                                    localDstStateObjects[i].lock();
                                    transactionDefinition.applyPhase.apply(
                                            Arrays.asList(combinedSrcStateObject),
                                            localDstStateObjects[i]
                                    );
                                    localDstStateObjects[i].unlock();
                                }
                            } catch (Exception ex) {
                                throw new IllegalStateException(ex);
                            }
                        }).start();
                    }
                });

            } else {

                runtimeContext.netManager.addEventListener(TransactionPushRequestEvent.TRANSACTION_REQUEST + transactionName, event -> {
                        final TransactionPushRequestEvent request = (TransactionPushRequestEvent) event;
                        final List<Object> preparedSrcStateObjects = request.srcStateObjectsValues;
                        new Thread(() -> {
                            try {
                                for (int i = 0; i < controller.getTransactionDescriptor().stateDstObjectNames.size(); ++i) {
                                    localDstStateObjects[i].lock();
                                    transactionDefinition.applyPhase.apply(
                                            Arrays.asList(preparedSrcStateObjects.get(i)),
                                            localDstStateObjects[i]
                                    );
                                    localDstStateObjects[i].unlock();
                                }
                            } catch (Exception ex) {
                                throw new IllegalStateException(ex);
                            }
                        });
                });
            }
        }
    }
}
