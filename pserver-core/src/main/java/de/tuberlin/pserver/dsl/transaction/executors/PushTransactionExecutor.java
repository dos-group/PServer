package de.tuberlin.pserver.dsl.transaction.executors;

import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.events.TransactionPushRequestEvent;
import de.tuberlin.pserver.dsl.transaction.phases.Prepare;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.runtime.RuntimeContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PushTransactionExecutor extends TransactionExecutor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final TransactionDefinition transactionDefinition;

    private final SharedObject[] localStateObjects;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PushTransactionExecutor(final RuntimeContext runtimeContext,
                                   final TransactionController controller) {

        super(runtimeContext, controller);

        this.transactionDefinition = controller.getTransactionDescriptor().definition;

        final int numStateObjects = controller.getTransactionDescriptor().stateObjectNames.size();

        this.localStateObjects = new SharedObject[numStateObjects];

        register();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void bind() throws Exception {

        int i = 0;

        for (final String stateObjectName : controller.getTransactionDescriptor().stateObjectNames) {
            localStateObjects[i++] = runtimeContext.runtimeManager.getDHT(stateObjectName);
        }
    }

    @Override
    public synchronized List<Object> execute(final Object requestObject) throws Exception {

        final List<Object> preparedStateObjects = new ArrayList<>();

        for (int i = 0; i < controller.getTransactionDescriptor().stateObjectNames.size(); ++i) {

            localStateObjects[i].lock();
            final Prepare preparePhase = transactionDefinition.preparePhase;
            preparedStateObjects.add((preparePhase != null) ? preparePhase.prepare(localStateObjects[i]) : localStateObjects[i]);
            localStateObjects[i].unlock();
        }

        final TransactionPushRequestEvent request = new TransactionPushRequestEvent(
                transactionName,
                controller.getTransactionDescriptor().stateObjectNames,
                preparedStateObjects,
                requestObject,
                controller.getTransactionDescriptor().cacheRequestObject
        );

        runtimeContext.netManager.dispatchEventAt(controller.getTransactionDescriptor().stateObjectNodes, request);

        return null;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void register() {
        runtimeContext.netManager.addEventListener(TransactionPushRequestEvent.TRANSACTION_REQUEST + transactionName, event -> {

            try {

                final TransactionPushRequestEvent request = (TransactionPushRequestEvent) event;

                final List<Object> preparedStateObjects = request.stateObjectsValues;

                for (int i = 0; i < controller.getTransactionDescriptor().stateObjectNames.size(); ++i) {

                    localStateObjects[i].lock();

                    transactionDefinition.applyPhase.apply(Arrays.asList(preparedStateObjects.get(i)), localStateObjects[i]);

                    localStateObjects[i].unlock();
                }

            } catch (Exception ex) {

                throw new IllegalStateException(ex);
            }
        });
    }
}
