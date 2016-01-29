package de.tuberlin.pserver.dsl.transaction;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.compiler.TransactionDescriptor;
import de.tuberlin.pserver.dsl.transaction.executors.TransactionExecutor;
import de.tuberlin.pserver.runtime.RuntimeContext;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Timer;
import java.util.TimerTask;

public final class TransactionController {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final RuntimeContext runtimeContext;

    private final TransactionDescriptor transactionDescriptor;

    private final TransactionExecutor transactionExecutor;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionController(final RuntimeContext runtimeContext,
                                 final TransactionDescriptor transactionDescriptor) {

        this.runtimeContext = Preconditions.checkNotNull(runtimeContext);
        this.transactionDescriptor = Preconditions.checkNotNull(transactionDescriptor);
        this.transactionExecutor = TransactionExecutor.create(transactionDescriptor.type, runtimeContext, this);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public TransactionDescriptor getTransactionDescriptor() { return transactionDescriptor; }

    public void bindTransaction() throws Exception {
        transactionExecutor.bind();
        activateTransactionObserver();
    }

    public Object executeTransaction(final Object requestObject) throws Exception {
        return transactionExecutor.execute(requestObject);
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void activateTransactionObserver() {

        if (transactionDescriptor.observerPeriod != -1
                && transactionDescriptor.definition.transactionObserver != null
                && ArrayUtils.contains(transactionDescriptor.srcStateObjectNodes, runtimeContext.nodeID)) {

            new Timer(true).scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    try {
                        if (transactionDescriptor.definition.transactionObserver.observe()) {
                            // TODO: No txn request object supported at the moment.
                            // TODO: No txn results supported at the moment.
                            transactionExecutor.execute(null);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

            }, 0, transactionDescriptor.observerPeriod);
        }
    }
}
