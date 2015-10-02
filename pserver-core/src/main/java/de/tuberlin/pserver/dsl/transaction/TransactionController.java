package de.tuberlin.pserver.dsl.transaction;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.transaction.executors.TransactionExecutor;
import de.tuberlin.pserver.runtime.RuntimeContext;

public class TransactionController {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final TransactionDescriptor transactionDescriptor;

    private final TransactionExecutor transactionExecutor;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionController(final RuntimeContext runtimeContext,
                                 final TransactionDescriptor transactionDescriptor) {

        this.transactionDescriptor = Preconditions.checkNotNull(transactionDescriptor);

        this.transactionExecutor = TransactionExecutor.create(transactionDescriptor.type, runtimeContext, this);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public TransactionDescriptor getTransactionDescriptor() { return transactionDescriptor; }

    public Object executeTransaction(final Object requestObject) throws Exception {
        return transactionExecutor.execute(requestObject);
    }
}
