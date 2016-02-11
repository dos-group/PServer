package de.tuberlin.pserver.dsl.transaction.executors;

import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.events.TransactionPushRequestEvent;
import de.tuberlin.pserver.dsl.transaction.phases.Prepare;
import de.tuberlin.pserver.runtime.RuntimeContext;
import de.tuberlin.pserver.types.metadata.DistributedType;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PushTransactionExecutor extends TransactionExecutor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final TransactionDefinition transactionDefinition;

    private final DistributedType[] srcStateObjects;

    private final DistributedType[] dstStateObjects;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PushTransactionExecutor(final RuntimeContext runtimeContext,
                                   final TransactionController controller) {

        super(runtimeContext, controller);
        this.transactionDefinition = controller.getTransactionDescriptor().definition;
        final int numSrcStateObjects = controller.getTransactionDescriptor().stateSrcObjectNames.size();
        this.srcStateObjects = new DistributedType[numSrcStateObjects];
        final int numDstStateObjects = controller.getTransactionDescriptor().stateDstObjectNames.size();
        this.dstStateObjects = new DistributedType[numDstStateObjects];
        registerPushTransactionRequest();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public Object[] getSrcObjects() { return srcStateObjects; }

    @Override
    public Object[] getDstObjects() { return dstStateObjects; }

    @Override
    public void bind() throws Exception {
        int i = 0;
        for (final String srcStateObjectName : controller.getTransactionDescriptor().stateSrcObjectNames) {
            if (ArrayUtils.contains(controller.getTransactionDescriptor().srcStateObjectNodes, runtimeContext.nodeID)) {
                DistributedType srcObj = runtimeContext.runtimeManager.getDHT(srcStateObjectName);
                srcStateObjects[i++] = srcObj;
            }
        }
        i = 0;
        for (final String dstStateObjectName : controller.getTransactionDescriptor().stateDstObjectNames) {
            if (ArrayUtils.contains(controller.getTransactionDescriptor().dstStateObjectNodes, runtimeContext.nodeID)) {
                dstStateObjects[i++] = runtimeContext.runtimeManager.getDHT(dstStateObjectName);
            }
        }
    }

    @Override
    public synchronized List<Object> execute(final Object requestObject) throws Exception {

        // --------------------------------------------------

        int[] txnDstNodes = ArrayUtils.removeElements(
                controller.getTransactionDescriptor().dstStateObjectNodes,
                runtimeContext.nodeID
        );

        if (txnDstNodes.length == 0)
            return null;

        // --------------------------------------------------

        //
        // Apply prepare phase on src-state object.
        //

        final List<Object> preparedSrcStateObjects = new ArrayList<>();
        for (int i = 0; i < controller.getTransactionDescriptor().stateSrcObjectNames.size(); ++i) {
            srcStateObjects[i].lock();
            final Prepare preparePhase = transactionDefinition.preparePhase;
            preparedSrcStateObjects.add((preparePhase != null) ? preparePhase.prepare(requestObject, srcStateObjects[i]) : srcStateObjects[i]);
            srcStateObjects[i].unlock();
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

            if (transactionDefinition.combinePhase != null) { // USE COMBINER -> synchronous

                AtomicInteger requestIndex = new AtomicInteger(0);
                int numRequests = controller.getTransactionDescriptor().srcStateObjectNodes.length - 1;
                List<Object> srcStateObjects = new ArrayList<>(numRequests);
                List<Object> srcRequestObjects = new ArrayList<>(numRequests);

                runtimeContext.netManager.addEventListener(TransactionPushRequestEvent.TRANSACTION_REQUEST + transactionName, event -> {
                    final TransactionPushRequestEvent request = (TransactionPushRequestEvent) event;
                    srcStateObjects.addAll(request.srcStateObjectsValues);
                    srcRequestObjects.add(request.requestObject);
                    int requestCounter = requestIndex.incrementAndGet();

                    System.out.println("NODE [" + runtimeContext.nodeID + "] RECEIVED " + requestCounter + " PUSH REQUESTS.");

                    if (requestCounter == numRequests) {
                        new Thread(() -> {
                            try {
                                Object combinedSrcStateObject = transactionDefinition.combinePhase.combine(srcRequestObjects, srcStateObjects);
                                for (int i = 0; i < controller.getTransactionDescriptor().stateDstObjectNames.size(); ++i) {
                                    dstStateObjects[i].lock();
                                    transactionDefinition.applyPhase.apply(srcRequestObjects,
                                            Arrays.asList(combinedSrcStateObject),
                                            dstStateObjects[i]
                                    );
                                    dstStateObjects[i].unlock();
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
                                    dstStateObjects[i].lock();
                                    transactionDefinition.applyPhase.apply(Arrays.asList(request.requestObject),
                                            Arrays.asList(preparedSrcStateObjects.get(i)),
                                            dstStateObjects[i]
                                    );
                                    dstStateObjects[i].unlock();
                                }
                            } catch (Exception ex) {
                                throw new IllegalStateException(ex);
                            }
                        }).start();
                });
            }
        }
    }
}
