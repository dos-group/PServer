package de.tuberlin.pserver.dsl.transaction;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

public final class TransactionMng {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static ProgramContext programContext;

    private static TransactionBuilder transactionBuilder;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public static void setProgramContext(final ProgramContext programContext) {
        TransactionMng.programContext = Preconditions.checkNotNull(programContext);
        TransactionMng.transactionBuilder = new TransactionBuilder(TransactionMng.programContext);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static <T> T commit(final TransactionDefinition transactionDefinition) throws Exception{
        return commit(transactionDefinition, null);
    }

    @SuppressWarnings("unchecked")
    public static <T> T commit(final TransactionDefinition transactionDefinition, final Object requestObject) throws Exception{
        final String transactionName = transactionDefinition.getTransactionName();
        final TransactionController controller = programContext.programTable.getTransactionController(transactionName);
        Preconditions.checkState(controller != null);
        return (T)controller.executeTransaction(requestObject);
    }

    public static TransactionBuilder getTransactionBuilder() {
        transactionBuilder.clear();
        return transactionBuilder;
    }
}
