package de.tuberlin.pserver.dsl.transaction;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.transaction.executors.TransactionExecutor;
import de.tuberlin.pserver.runtime.RuntimeContext;

public class TransactionController {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final TransactionDeclaration declaration;

    private final TransactionDefinition definition;

    private final TransactionExecutor transactionExecutor;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TransactionController(final RuntimeContext runtimeContext,
                                 final TransactionDeclaration declaration,
                                 final TransactionDefinition definition) {

        this.declaration = Preconditions.checkNotNull(declaration);

        this.definition  = Preconditions.checkNotNull(definition);

        this.transactionExecutor = TransactionExecutor.create(declaration.type, runtimeContext, this);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public TransactionDefinition getTransactionDefinition() { return definition; }

    public TransactionDeclaration getTransactionDeclaration() { return declaration; }

    public void executeTransaction() throws Exception {
        transactionExecutor.execute();
    }
}
