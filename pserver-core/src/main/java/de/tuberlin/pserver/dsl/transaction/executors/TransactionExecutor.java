package de.tuberlin.pserver.dsl.transaction.executors;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.dsl.transaction.annotations.TransactionType;
import de.tuberlin.pserver.runtime.RuntimeContext;

import java.util.List;


public abstract class TransactionExecutor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final String transactionName;

    protected final RuntimeContext runtimeContext;

    protected final TransactionController controller;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionExecutor(final RuntimeContext runtimeContext,
                               final TransactionController controller) {

        this.runtimeContext = Preconditions.checkNotNull(runtimeContext);

        this.controller = Preconditions.checkNotNull(controller);

        this.transactionName = controller.getTransactionDescriptor().transactionName;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void bind() throws Exception;

    public abstract List<Object> execute(final Object requestObject) throws Exception;

    public abstract Object[] getSrcObjects();

    public abstract Object[] getDstObjects();

    // ---------------------------------------------------
    // Factory.
    // ---------------------------------------------------

    public static TransactionExecutor create(final TransactionType type,
                                             final RuntimeContext runtimeContext,
                                             final TransactionController controller) {
        switch (type) {
            case PUSH: return new PushTransactionExecutor(runtimeContext, controller);
            case PULL: return new PullTransactionExecutor(runtimeContext, controller);
        }
        return null;
    }
}
