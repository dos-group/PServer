package de.tuberlin.pserver.dsl.transaction.executors;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.dsl.transaction.TransactionType;
import de.tuberlin.pserver.runtime.RuntimeContext;


public abstract class TransactionExecutor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected static final String PULL_WRITE_TRANSACTION_REQUEST  = "pull_write_transaction_request";

    protected static final String PULL_WRITE_TRANSACTION_RESPONSE = "pull_write_transaction_response";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected RuntimeContext runtimeContext;

    protected TransactionController controller;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionExecutor(final RuntimeContext runtimeContext,
                               final TransactionController controller) {

        this.runtimeContext = Preconditions.checkNotNull(runtimeContext);

        this.controller = Preconditions.checkNotNull(controller);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void execute() throws Exception;

    // ---------------------------------------------------
    // Factory.
    // ---------------------------------------------------

    public static TransactionExecutor create(final TransactionType type,
                                             final RuntimeContext runtimeContext,
                                             final TransactionController controller) {
        switch (type) {
            case PUSH_WRITE:
                break;
            case PULL_WRITE: return new PullWriteExecutor(runtimeContext, controller);
            case READ:
                break;
        }
        return null;
    }
}
