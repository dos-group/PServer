package de.tuberlin.pserver.dsl.transaction.executors;

import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.dsl.transaction.phases.Prepare;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.runtime.RuntimeContext;
import de.tuberlin.pserver.runtime.dht.types.EmbeddedDHTObject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class PullWriteExecutor extends TransactionExecutor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final String requestTransactionID;

    private final String responseTransactionID;

    private final List<Object> resultObjects = new ArrayList<>();

    private CountDownLatch responseLatch;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PullWriteExecutor(final RuntimeContext runtimeContext,
                             final TransactionController controller) {

        super(runtimeContext, controller);
        final String name = controller.getTransactionDeclaration().transactionName;
        this.requestTransactionID  = PULL_WRITE_TRANSACTION_REQUEST + "_" + name;
        this.responseTransactionID = PULL_WRITE_TRANSACTION_RESPONSE + "_" + name;
        register();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public synchronized void execute() throws Exception {
        resultObjects.clear();
        final NetEvents.NetEvent request = new NetEvents.NetEvent(requestTransactionID);
        runtimeContext.netManager.sendEvent(controller.getTransactionDeclaration().nodes, request);
        responseLatch = new CountDownLatch(controller.getTransactionDeclaration().nodes.length);
        responseLatch.await();
        controller.getTransactionDefinition().applyPhase.apply(resultObjects);
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void register() {

        runtimeContext.netManager.addEventListener(requestTransactionID, event -> {
            final NetEvents.NetEvent request = (NetEvents.NetEvent)event;
            try {
                final SharedObject stateObject = ((EmbeddedDHTObject)runtimeContext.dataManager.getLocal(controller.getTransactionDeclaration().stateName)[0]).object;
                stateObject.lock();
                final Prepare preparePhase = controller.getTransactionDefinition().preparePhase;
                final Object preparedStateObject = (preparePhase != null) ? preparePhase.prepare(stateObject) : stateObject;
                final NetEvents.NetEvent response = new NetEvents.NetEvent(responseTransactionID);
                response.setPayload(preparedStateObject);
                runtimeContext.netManager.sendEvent(request.srcMachineID, response);
                stateObject.unlock();
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        });

        runtimeContext.netManager.addEventListener(responseTransactionID, event -> {
            final NetEvents.NetEvent response = (NetEvents.NetEvent)event;
            resultObjects.add(response.getPayload());
            responseLatch.countDown();
        });
    }

}
