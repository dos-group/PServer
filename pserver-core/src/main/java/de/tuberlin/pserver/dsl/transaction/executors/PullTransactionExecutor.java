package de.tuberlin.pserver.dsl.transaction.executors;

import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.events.TransactionPullRequestEvent;
import de.tuberlin.pserver.dsl.transaction.events.TransactionPullResponseEvent;
import de.tuberlin.pserver.dsl.transaction.phases.Prepare;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.runtime.RuntimeContext;
import org.apache.commons.lang3.ArrayUtils;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

// EXECUTOR ASSUMES NO CONCURRENT TRANSACTIONS OF SAME TYPE.
// MUST BE ENFORCED BY TRANSACTION MANAGER!

public class PullTransactionExecutor extends TransactionExecutor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Object monitor = new Object();

    // ---------------------------------------------------

    private final TransactionDefinition transactionDefinition;

    private final Map<String, List<Object>> collectedResponseSrcStateObjects = new HashMap<>();

    private final SharedObject[] srcStateObjects;

    private final SharedObject[] dstStateObjects;

    private final int[] txnSrcNodes;

    private CountDownLatch responseLatch;

    private final Thread combinerThread;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    public PullTransactionExecutor(final RuntimeContext runtimeContext,
                                   final TransactionController controller) {

        super(runtimeContext, controller);

        this.transactionDefinition = controller.getTransactionDescriptor().definition;
        final int numSrcStateObjects = controller.getTransactionDescriptor().stateSrcObjectNames.size();
        this.srcStateObjects = new SharedObject[numSrcStateObjects];
        final int numDstStateObjects = controller.getTransactionDescriptor().stateDstObjectNames.size();
        this.dstStateObjects = new SharedObject[numDstStateObjects];

        txnSrcNodes = ArrayUtils.removeElements(
                controller.getTransactionDescriptor().srcStateObjectNodes,
                runtimeContext.nodeID
        );

        this.responseLatch = new CountDownLatch(txnSrcNodes.length);

        combinerThread = new Thread(() -> { // RUN COMBINER IN SEPARATE THREAD!
            try {

                for (String srcStateName : controller.getTransactionDescriptor().stateSrcObjectNames) {
                    List<Object> srcStates = collectedResponseSrcStateObjects.get(srcStateName);
                    Object combinedSrcStateObject = transactionDefinition.combinePhase.combine(srcStates);
                    collectedResponseSrcStateObjects.put(srcStateName, Arrays.asList(combinedSrcStateObject));
                }

            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });

        registerPullTransactionRequest();
        registerPullTransactionResponse();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void bind() throws Exception {
        int i = 0;
        for (final String srcStateObjectName : controller.getTransactionDescriptor().stateSrcObjectNames) {
            if (ArrayUtils.contains(controller.getTransactionDescriptor().srcStateObjectNodes, runtimeContext.nodeID)) {
                SharedObject srcObj = runtimeContext.runtimeManager.getDHT(srcStateObjectName);
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
    @SuppressWarnings("unchecked")
    public synchronized List<Object> execute(final Object requestObject) throws Exception {

        if (txnSrcNodes.length == 0)
            return null;

        collectedResponseSrcStateObjects.clear();
        final List<Object> resultObjects = new ArrayList<>();

        { // Request...

            final boolean cacheRequest = controller.getTransactionDescriptor().cacheRequestObject;
            final TransactionPullRequestEvent request = new TransactionPullRequestEvent(
                    transactionName,
                    controller.getTransactionDescriptor().stateSrcObjectNames,
                    requestObject,
                    cacheRequest
            );

            runtimeContext.netManager.dispatchEventAt(txnSrcNodes, request);
        }

        { // Await Response...

            responseLatch.await();
            synchronized (monitor) {
                responseLatch = new CountDownLatch(txnSrcNodes.length);
            }
        }

        { // Response Handling...

            // Wait until combiner thread is completed.
            if (transactionDefinition.combinePhase != null) {
                combinerThread.join();
            }

            for (int i = 0; i < controller.getTransactionDescriptor().stateDstObjectNames.size(); ++i) {
                resultObjects.add(
                        transactionDefinition.applyPhase.apply(
                                collectedResponseSrcStateObjects.get(controller.getTransactionDescriptor().stateSrcObjectNames.get(i)),
                                dstStateObjects[i]
                        )
                );
            }
        }

        return resultObjects;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    private void registerPullTransactionRequest() {

        // Register push request listener only at the associated destination nodes.
        if (ArrayUtils.contains(controller.getTransactionDescriptor().srcStateObjectNodes, runtimeContext.nodeID)) {

            runtimeContext.netManager.addEventListener(TransactionPullRequestEvent.TRANSACTION_REQUEST + transactionName, event -> {
                final TransactionPullRequestEvent request = (TransactionPullRequestEvent) event;

                try {

                    final Map<String, Object> preparedOutputs = new HashMap<>();
                    for (int i = 0; i < request.stateObjectNames.size(); ++i) {
                        srcStateObjects[i].lock();
                        final Prepare preparePhase = transactionDefinition.preparePhase;
                        final Object prepareInput = request.getPayload() == null ? srcStateObjects[i] : request.requestObject;
                        preparedOutputs.put(request.stateObjectNames.get(i), (preparePhase != null) ? preparePhase.prepare(prepareInput) : prepareInput);
                        srcStateObjects[i].unlock();
                    }

                    runtimeContext.netManager.dispatchEventAt(
                            request.srcMachineID,
                            new TransactionPullResponseEvent(transactionName, preparedOutputs)
                    );

                } catch (Exception ex) {
                    throw new IllegalStateException(ex);
                }
            });
        }
    }

    // NO GUARANTEE ABOUT RESPONSE ORDER IS EQUAL TO REQUEST ORDER!

    private void registerPullTransactionResponse() {

        // Register push request listener only at the associated destination nodes.
        if (ArrayUtils.contains(controller.getTransactionDescriptor().dstStateObjectNodes, runtimeContext.nodeID)) {

            AtomicInteger requestCounter = new AtomicInteger(0);

            runtimeContext.netManager.addEventListener(TransactionPullResponseEvent.TRANSACTION_RESPONSE + transactionName, event -> {

                final TransactionPullResponseEvent responseEvent = (TransactionPullResponseEvent) event;

                try {

                    for (int i = 0; i < controller.getTransactionDescriptor().stateSrcObjectNames.size(); ++i) {
                        final String stateObjectName = controller.getTransactionDescriptor().stateSrcObjectNames.get(i);
                        List<Object> li = collectedResponseSrcStateObjects.get(stateObjectName);
                        if (li == null) {
                            li = new ArrayList<>();
                            collectedResponseSrcStateObjects.put(stateObjectName, li);
                        }
                        li.add(responseEvent.responseSrcStateObjects.get(stateObjectName));
                    }

                    if (transactionDefinition.combinePhase != null) {
                        if (requestCounter.incrementAndGet() == txnSrcNodes.length) {
                            combinerThread.start();
                        }
                    }

                    synchronized (monitor) {
                        responseLatch.countDown();
                    }

                } catch(Exception ex) {
                    throw new IllegalStateException(ex);
                }
            });
        }
    }
}
